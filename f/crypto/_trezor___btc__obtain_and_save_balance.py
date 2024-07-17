import time
from datetime import datetime, timezone
from typing import Optional

import logfire
import requests
import wmill
from hdwallet import HDWallet
from hdwallet.symbols import BTC as SYMBOL
from pydantic import BaseModel
from pymongo import MongoClient


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


BLOCKCHAIN_BASE_URL = "https://blockchain.info/q/addressbalance/"
BTC_PRICE_URL = "https://api.coinbase.com/v2/prices/BTC-EUR/spot"
XPUB_ADDRESS_GAP = 1


def main():
    logfire.configure()

    with logfire.span("Job: get_trezor_btc_wallet_balance - Status: Running"):
        btc_xpub = wmill.get_variable("f/crypto/btc_xpub")
        mongo_dsn = wmill.get_variable("f/general/mongo_dsn")
        hdwallet: HDWallet = HDWallet(symbol=SYMBOL)
        hdwallet.from_xpublic_key(xpublic_key=btc_xpub)

        found_first_address = False

        mongo_client = MongoClient(mongo_dsn)
        finance_db = mongo_client.finance

        wallet_address_indexes_collection = finance_db.wallet_address_indexes
        wallet_address_indexes_collection.create_index("xpub")

        if "wallet_balances" not in finance_db.list_collection_names():
            wallet_balances_collection = finance_db.create_collection(
                "wallet_balances", timeseries={"timeField": "timestamp"}
            )
        else:
            wallet_balances_collection = finance_db.wallet_balances

        first_index = wallet_address_indexes_collection.find_one({"xpub": btc_xpub})
        index = 0
        if first_index:
            index = first_index["index"]

        empty_count = 0
        total_balance = 0
        while empty_count < XPUB_ADDRESS_GAP:
            hdwallet.clean_derivation()
            path = f"m/0/{index}"
            hdwallet.from_path(path)
            address = hdwallet.p2wpkh_address()
            response = requests.get(f"{BLOCKCHAIN_BASE_URL}/{address}").text
            balance = int(response)

            if not found_first_address:
                wallet_address_indexes_collection.update_one(
                    {"xpub": btc_xpub}, {"$set": {"index": index}}, upsert=True
                )

            if not balance:
                if found_first_address:
                    empty_count += 1
            else:
                if not found_first_address:
                    found_first_address = True

                total_balance += balance
                empty_count = 0
            index += 1
            time.sleep(10)

        response = requests.get(BTC_PRICE_URL).json()
        price = float(response["data"]["amount"])

        balance = WalletBalance(
            wallet_name="Trezor - BTC",
            coin_symbol="BTC",
            coin_unit="SAT",
            coin_amount=total_balance,
            coin_decimals=8,
            fiat_symbol="EUR",
            fiat_unit="EUR",
            coin_price_in_fiat=price,
            timestamp=datetime.now(timezone.utc),
        )
        wallet_balances_collection.insert_one(balance.model_dump())
        logfire.info(
            "Job: get_trezor_btc_wallet_balance - Status: Successfully executed"
        )
