from typing import Union
import asyncio
from datetime import datetime
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from databases import Database
import schema
import sqlalchemy

database = Database("sqlite:///transactions.db")

app = FastAPI()

@app.on_event("startup")
async def database_connect():
    await database.connect()


@app.on_event("shutdown")
async def database_disconnect():
    await database.disconnect()

@app.get('/transactions/{hash}', response_model= schema.Transaction)
async def fetch_transaction(
    hash: str
):
    query = "select hash, from_address, to_address, block_number, block_timestamp, receipt_gas_used, gas_cost_usd from transactions where hash = {}".format(str(hash))
    tx = await database.fetch_all(query)
    record = tx[0]

    res = {
        "hash" : record[0],
        "fromAddress" : record[1],
        "toAddress" : record[2],
        "blockNumber" : record[3],
        "executedAt" : datetime.utcfromtimestamp(int(record[4])).strftime("%b-%d-%Y %I:%M:%S %p %Z +UTC"),
        "gasUsed" : float(record[5]),
        "gasCostInDollars" : record[6]
    }

    return res
    
    
