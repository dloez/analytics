from datetime import datetime, timedelta, timezone
from math import pow
from typing import Optional

import logfire
import wmill
from pydantic import BaseModel
from pymongo import MongoClient
from pymongo.collection import Collection
from pytz import UTC


class WalletBalance(BaseModel):
    wallet_name: str
    wallet_address: Optional[str] = None
    coin_symbol: str
    coin_unit: str
    coin_amount: float
    coin_decimals: int
    fiat_symbol: str
    fiat_unit: str
    coin_price_in_fiat: float
    timestamp: datetime


class TotalWalletBalances(BaseModel):
    from_wallet_names: list[str]
    balance_in_fiat: float
    fiat_symbol: str
    fiat_unit: str
    timestamp: datetime


def round_to_nearest_5min(dt: datetime):
    minutes = dt.minute
    minutes_to_subtract = minutes % 5
    rounded_dt = dt - timedelta(minutes=minutes_to_subtract)
    rounded_dt = rounded_dt.replace(second=0, microsecond=0)
    return rounded_dt


def find_latest_balance_from_timestamp(
    wallet_name: str, timestamp: datetime, collection: Collection
) -> WalletBalance | int:
    balance = collection.find_one(
        {"timestamp": {"$lte": timestamp}, "wallet_name": wallet_name},
        sort=[("timestamp", -1)],
    )
    if not balance:
        return 0
    return WalletBalance.model_validate(balance)


def main():
    logfire.configure()
    wallet_names = ["Trezor - BTC", "Trezor - ETH"]

    with logfire.span("Job: calculate_total_crypto_balance - Status: Running"):
        mongo_dsn = wmill.get_variable("f/general/mongo_dsn")
        mongo_client = MongoClient(mongo_dsn)
        finance_db = mongo_client.finance

        if "wallet_balances" not in finance_db.list_collection_names():
            wallet_balances_collection = finance_db.create_collection(
                "wallet_balances", timeseries={"timeField": "timestamp"}
            )
        else:
            wallet_balances_collection = finance_db.wallet_balances
        total_wallet_balances_collection = finance_db.total_wallet_balances

        oldest_balance = WalletBalance.model_validate(
            wallet_balances_collection.find_one({})
        )
        oldest_timestamp = UTC.localize(round_to_nearest_5min(oldest_balance.timestamp))
        current_time = round_to_nearest_5min(datetime.now(timezone.utc))
        iter_count = 0
        calculated_count = 0
        while current_time > oldest_timestamp:
            iter_count += 1
            prev_time = current_time - timedelta(minutes=5)

            prev_total_wallet_balance = total_wallet_balances_collection.find_one(
                {"timestamp": current_time}
            )
            if prev_total_wallet_balance:
                current_time = prev_time
                continue

            balances = wallet_balances_collection.find(
                {"timestamp": {"$lte": current_time, "$gt": prev_time}}
            ).limit(100)

            coin_amounts_in_fiat = {}
            for balance in balances:
                balance = WalletBalance.model_validate(balance)
                if balance.wallet_name not in coin_amounts_in_fiat:
                    coin_amounts_in_fiat[balance.wallet_name] = []

                coin_amounts_in_fiat[balance.wallet_name].append(
                    (
                        balance.coin_amount
                        / pow(10, balance.coin_decimals)
                        * balance.coin_price_in_fiat
                    )
                )

            missing_wallets = []
            for wallet_name in wallet_names:
                if wallet_name not in coin_amounts_in_fiat:
                    missing_wallets.append(wallet_name)

            for missing_wallet in missing_wallets:
                missing_balance = find_latest_balance_from_timestamp(
                    wallet_name=missing_wallet,
                    timestamp=current_time,
                    collection=wallet_balances_collection,
                )
                coin_amount_in_fiat = missing_balance
                if coin_amount_in_fiat != 0:
                    coin_amount_in_fiat = (
                        missing_balance.coin_amount
                        / pow(10, missing_balance.coin_decimals)
                        * missing_balance.coin_price_in_fiat
                    )
                coin_amounts_in_fiat[missing_wallet] = [coin_amount_in_fiat]

            if not coin_amounts_in_fiat:
                current_time = prev_time
                continue

            totals = []
            for _, v in coin_amounts_in_fiat.items():
                totals.append(sum(v) / len(v))

            total = sum(totals)
            total_wallet_balance = TotalWalletBalances(
                balance_in_fiat=total,
                fiat_symbol=balance.fiat_symbol,
                fiat_unit=balance.fiat_unit,
                from_wallet_names=coin_amounts_in_fiat.keys(),
                timestamp=current_time,
            )
            total_wallet_balances_collection.insert_one(
                total_wallet_balance.model_dump()
            )
            calculated_count += 1
            current_time = prev_time
        logfire.info(
            f"Iterated over {iter_count} time intervals, calculated {calculated_count} balances"
        )
        logfire.info(
            "Job: calculate_total_crypto_balance - Status: Successfully executed"
        )
