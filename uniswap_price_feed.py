from web3 import Web3
from web3.middleware import geth_poa_middleware
from dotenv import load_dotenv
import os 
load_dotenv()
import pandas as pd
import itertools

RPC = os.getenv('RPC')

def hex_to_int(s):
    return int(s, 16)

def get_uni_swaps(
    block_numbers: list
    ) -> Union[List, Dict]:
    w3 = Web3(Web3.HTTPProvider(RPC))
    w3.middleware_onion.inject(geth_poa_middleware, layer=0)  #  Inject poa middleware

    assert w3.isConnected(), "w3 not connected"

    address = '0xB4e16d0168e52d35CaCD2c6185b44281Ec28C9Dc'
    topics = ['0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822']

    minim = min(block_numbers)
    maxim = max(block_numbers)
    
    logs = []
    
    start_block = minim

    if maxim - minim > 2048:
        end_block = start_block + 2048
    else:
        end_block = maxim

    while end_block - start_block > 0:
        print("start: " + str(start_block))
        print("end: " + str(end_block))
        print("diff: " + str(end_block - start_block))
        filter_params = {
            "address": address,
            "topics": topics,
            "fromBlock": start_block, 
            "toBlock": end_block
        }

        data = w3.eth.get_logs(filter_params)
        logs.append(data)

        start_block = end_block
        if maxim - end_block < 2048:
            end_block = maxim
        else:
            end_block = end_block + 2048
    
    return logs

def parser(
    df: pd.DataFrame
) -> pd.DataFrame:

    swaps = df
    data = df.data.to_list()
    swaps['amount_in0'] = swaps.data.apply(lambda x: hex_to_int(x[2:66]) / 1e6)
    swaps['amount_in1'] = swaps.data.apply(lambda x: hex_to_int(x[67:(66+64)]) / 1e18)
    swaps['amount_out0'] = swaps.data.apply(lambda x: hex_to_int(x[(1+67+64):(66+64*2)]) / 1e6)
    swaps['amount_out1'] = swaps.data.apply(lambda x: hex_to_int(x[(1+67+64*2):(66+64*3)]) / 1e18)

    swaps['eth_per_usd'] = swaps['amount_out1']/swaps['amount_in0']
    swaps['usd_per_eth'] = swaps['amount_in0']/swaps['amount_out1']
    swaps.loc[swaps['amount_in0'] == 0 , "eth_per_usd"] = swaps['amount_in1']/swaps['amount_out0']
    swaps.loc[swaps['amount_in0'] == 0 , "usd_per_eth"] = swaps['amount_out0']/swaps['amount_in1']

    return swaps
