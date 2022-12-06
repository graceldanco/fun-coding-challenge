import pandas as pd 
import asyncio
import sqlite3 as sq 
import pytest
from web3 import Web3
from web3.middleware import geth_poa_middleware
import datetime
import time
from pycoingecko import CoinGeckoAPI
import aiohttp
import requests
import csv
import json
import os
import itertools
from dotenv import load_dotenv
import uniswap_price_feed as uni 

load_dotenv()

"""
1. Need to calculate the block_timestamp from the block_number in the dataset 
    --> can use web3.py to get the block_timestamp but maybe multicall would be able to do it faster?

--> https://dev.to/narasimha1997/lets-build-a-simple-asynchronous-web3-client-for-ethereum-blockchain-in-python-45dh

- without any concurrency or batching it takes aroun 6 minutes to run 

2. Need to use async then to pull the price of eth from coingecko' API (why did they not decide to go with etherscan for this?)

3. Create a data table with added columns for block_timestamp, gas cost of each transaction in Gwei (1e-9 ETH) (gas_price / 1e9)

, $$ cost of gas used

4. Export this new dataframe to a sqlite database 

https://stackoverflow.com/questions/2887878/importing-a-csv-file-into-a-sqlite3-database-table-using-python

to get out of sqlite command line sqlite> press control D
"""


async def get_timestamps(
    session: aiohttp.ClientSession, 
    block_number: int 
    ) -> list:
    ts = w3.eth.get_block(int(block_number)) # Need to make this into a post request
    # print(block_number)
    # print(ts)
    return ts.timestamp


async def gather_timestamps(
    block_numbers: list
    ) -> list:

    print(datetime.datetime.now())

    async with aiohttp.ClientSession() as session:
        tasks: list = []
        for bl in block_numbers:
            block_timestamp = asyncio.create_task(get_timestamps(session, bl)) # ensure_future
            tasks.append(block_timestamp)
        # print(datetime.datetime.now())
        # print(data['block_number'].apply(lambda i: w3.eth.get_block(int(i)).timestamp))
        # print(datetime.datetime.now())
    print(datetime.datetime.now())
    data = await asyncio.gather(*tasks)
    return data

async def get_price_usd(
    session: aiohttp.ClientSession, 
    date: str
    ) -> list:

    # date = datetime.datetime.fromtimestamp(date).strftime("%d-%m-%Y")
    # print(date)
    url = f'https://api.coingecko.com/api/v3/coins/ethereum/history?date={date}' # coingecko allows 10 to 50 calls / minute for their free tier API so need to batch into groups of at least 100 (ideally 250) before running this
    async with session.get(url) as response:
        result = await response.json() 
    
        return result['market_data']['current_price']['usd']

async def get_prices_usd(
    session: aiohttp.ClientSession, 
    dates: list
    ) -> list:
    
    ordered_dates = sorted(dates,key=lambda d:  datetime.datetime.fromtimestamp(d).strftime("%d-%m-%Y: %H:%M:%S"))
    _from = ordered_dates[0]
    _to = ordered_dates[-1]
    url = f'https://api.coingecko.com/api/v3/coins/ethereum/market_chart/range?vs_currency=usd&from={_from}&to={_to}'

    async with session.get(url) as response:
        if response.status == 200:
            results = await response.json()
            # print(results)
        
            return results

async def gather_prices(
    dates: list
    ) -> list:

    async with aiohttp.ClientSession() as session:
        tasks: list = []
        cleaned_dates = [datetime.datetime.fromtimestamp(date).strftime("%d-%m-%Y") for date in dates]
        unique_dates = set(cleaned_dates)
        unique_dates = list(unique_dates)
        for date in unique_dates:
            price = asyncio.create_task(get_price_usd(session, date))
            tasks.append(price)
        
        data = await asyncio.gather(*tasks)     
        return data

def get_uniswap_data(_w3, block_numbers):

    """
        1. Need to pull all every time a usdc-eth swap occurs on Uni v2 : https://etherscan.io/tx/0x3d4cee789876f4e5540e5b64b3bd9a6d74e89d3a256cd035a98485b63502bae7
         -- between the blocks passed in from block_numbers
        2. Group by a minute by minute basis 
        3. Make sure that if there is gaps in the data pull from the previous row 
        4. Find the average( ratio of ETH / USDC ) for the swaps to compute the eth price in usd
    
        -- maybe could use multicall with this??
    """
    func = '0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822'

    filter_params = {
        "topic0": func
    }

def drop_db_table():
    
    con = sq.connect("transactions.db")
    cur = con.cursor()

    cur.execute("DROP TABLE transactions")

    cur.close()
    con.close()

def export_to_db(
    csv_file: str
    ) -> None:

    con = sq.connect("transactions.db")
    cur = con.cursor()
    
    # cur.execute("CREATE TABLE transactions (hash,nonce,transaction_index,from_address,to_address,value,gas,gas_price,input,receipt_cumulative_gas_used,receipt_gas_used,receipt_contract_address,receipt_root,receipt_status,block_number,block_hash,max_fee_per_gas,max_priority_fee_per_gas,transaction_type,receipt_effective_gas_price,block_timestamp,gas_cost, gas_cost_usd)") 

    with open(csv_file,'r') as file:
        reader = csv.DictReader(file) 
        to_db = [(i['hash'], i['nonce'], i['transaction_index'], i['from_address'],i['to_address'],i['value'],i['gas'],i['gas_price'],i['input'],i['receipt_cumulative_gas_used'],i['receipt_gas_used'], i['receipt_contract_address'], i['receipt_root'], i['receipt_status'], i['block_number'], i['block_hash'],i['max_fee_per_gas'],i['max_priority_fee_per_gas'],i['transaction_type'],i['receipt_effective_gas_price'],  i['block_timestamp'], i['gas_cost'], i['gas_cost_usd']) for i in reader]

    cur.executemany("INSERT INTO transactions (hash,nonce,transaction_index,from_address,to_address,value,gas,gas_price,input,receipt_cumulative_gas_used,receipt_gas_used,receipt_contract_address,receipt_root,receipt_status,block_number,block_hash,max_fee_per_gas,max_priority_fee_per_gas,transaction_type,receipt_effective_gas_price,block_timestamp,gas_cost, gas_cost_usd) VALUES (?, ?, ?, ?,?, ?,?, ?,?, ?,?, ?,?, ?,?, ?,?, ?,?, ?,?, ?,?);", to_db)
    con.commit()
    con.close()

if __name__ == "__main__":
    RPC_WS = os.getenv('RPC_WS')
    w3 = Web3(Web3.WebsocketProvider(RPC_WS))
    w3.middleware_onion.inject(geth_poa_middleware, layer=0)

    assert w3.isConnected()
    
    try:
        data = pd.read_csv('cleaned_transactions.csv')
    except:
        data = pd.read_csv('ethereum_txs.csv')

        blocks = data['block_number'].to_list()
        timestamps = asyncio.run(gather_timestamps(block_numbers=blocks))
        data['block_timestamp'] = timestamps
        data['gas_cost'] = data['gas_price'] / 1e9
        data.to_csv('cleaned_transactions.csv')

        data = pd.read_csv('cleaned_transactions.csv')

    print(data.columns)
    dates = pd.to_datetime(data.block_timestamp, unit='s').dt.strftime("%d-%m-%Y")
    data = data.sort_values('block_timestamp', ascending=True)
    uni_logs = uni.get_uni_swaps(data.block_number.to_list())
    flattened = list(itertools.chain.from_iterable(uni_logs))
    swap_logs = pd.DataFrame(flattened)
    parsed_logs = uni.parser(swap_logs)

    price_data = parsed_logs[['blockNumber', 'eth_per_usd', 'usd_per_eth']]
    combined_data = data.merge(price_data,how='inner', left_on='block_number', right_on='blockNumber')

    combined_data['gas_cost_usd'] = (combined_data['gas_cost']/1e9) * combined_data['usd_per_eth'] * combined_data['gas']
    
    final_data = combined_data.drop(['blockNumber', 'eth_per_usd', 'usd_per_eth'], axis=1)
    print(final_data)
    final_data.to_csv('final_data.csv')
    # drop_db_table()
    export_to_db('final_data.csv') #ONLY RUN IF YOU NEED TO UPDATE THE DB












    """ NEED TO FIX BELOW TO ACCOUNT FOR UNISWAP PRICE FEED"""
    # eth_prices = asyncio.run(gather_prices(dates=data['block_timestamp'].to_list()))
    # print(eth_prices)
    # data['dates'] = dates
    # data['eth_price'] = [eth_prices ]

    """ NEED TO FIGURE OUT HOW TO GET AT LEAST MINUTELY ETH PRICE DATA W COINGECKO OR ETHERSCAN FREE API """

    
    # prices_data = pd.DataFrame(eth_prices[0]['prices']).rename({0: 'block_timestamp', 1: 'eth_price'}, axis=1)
    # prices_data['datetime'] = pd.to_datetime(prices_data['block_timestamp'],unit='ms')
    # print(prices_data)
    # prices_data = prices_data.sort_values('block_timestamp')
    # data = data.sort_values('block_timestamp')

    # merged_df = pd.merge_asof(data, prices_data.assign(keydate=prices_data.datetime), left_on='block_timestamp', right_on='datetime', direction='forward')
    # print(merged_df)
    # merged_df['eth_price']=eth_prices
    # merged_df['gas_cost_usd'] = (data['gas_cost']/1e9)*data['eth_price']
    # merged_df.to_csv('cleaned_transactions.csv')
