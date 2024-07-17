from datetime import datetime, timezone
from typing import Optional

import logfire
import requests
import wmill
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


WALLET_BALANCE_URL = "https://api.blockchain.info/v2/eth/data/account/{}/wallet"
ETH_PRICE_URL = "https://api.coinbase.com/v2/prices/ETH-EUR/spot"


def main():
    logfire.configure()

    with logfire.span("Job: get_eth_wallet_balance - Status: Starting"):
        eth_address = wmill.get_variable("f/crypto/eth_address")
        mongo_dsn = wmill.get_variable("f/general/mongo_dsn")
        mongo_client = MongoClient(mongo_dsn)
        finance_db = mongo_client.finance

        if "wallet_balances" not in finance_db.list_collection_names():
            wallet_balances_collection = finance_db.create_collection(
                "wallet_balances", timeseries={"timeField": "timestamp"}
            )
        else:
            wallet_balances_collection = finance_db.wallet_balances

        response = requests.get(WALLET_BALANCE_URL.format(eth_address)).json()
        balance = float(response["balance"])
        decimals = 18

        response = requests.get(ETH_PRICE_URL).json()
        price = float(response["data"]["amount"])

        balance = WalletBalance(
            wallet_name="Trezor - ETH",
            wallet_address=eth_address,
            coin_symbol="ETH",
            coin_unit="WEI",
            coin_amount=balance,
            coin_decimals=decimals,
            fiat_symbol="EUR",
            fiat_unit="EUR",
            coin_price_in_fiat=price,
            timestamp=datetime.now(timezone.utc),
        )
        wallet_balances_collection.insert_one(balance.model_dump())
        logfire.info("Job: get_eth_wallet_balance - Status: Successfully executed")
