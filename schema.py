from typing import List, Optional
from pydantic import BaseModel, Extra
from datetime import datetime

"""
Example Response 

{
  "hash": "0xaaaaa",
  "fromAddress": "0x000000",
  "toAddress": "0x000001",
  "blockNumber": 1234,
  "executedAt": "Jul-04-2022 12:02:24 AM +UTC",
  "gasUsed": 12345678,
  "gasCostInDollars": 4.23,
}
"""

class Transaction(BaseModel):

    hash : str
    fromAddress : str
    toAddress : str
    blockNumber : int
    executedAt : str
    gasUsed : float
    gasCostInDollars : float

    class Config:
        extra = Extra.forbid



    


