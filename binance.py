from typing import Callable, Coroutine, List
import pandas as pd
import os
import aiohttp
import asyncio
import schedule
import time
import ccxt
import requests as req
from datetime import datetime
from operator import itemgetter
import nest_asyncio
nest_asyncio.apply()




async def http_get(session: aiohttp.ClientSession, url: str) -> Coroutine:
    """Execute an GET http call async """
    async with session.get(url) as response:
        resp = await response.json()
        return resp


async def http_post(session: aiohttp.ClientSession, url: str) -> Coroutine:
    """Execute an POST http call async """
    async with session.post(url) as response:
        resp = await response.json()
        return resp


async def fetch_all(urls: List, inner: Callable):
    """Gather many HTTP call made async """
    async with aiohttp.ClientSession() as session:
        tasks = []
        for url in urls:
            tasks.append(
                inner(
                    session,
                    url
                )
            )
        responses = await asyncio.gather(*tasks, return_exceptions=True)
        return responses

class Binance:

    def __init__(self):

        self.marketInfo = self.getAllSymbols()
        self.currentLimit = 0
        self.baseUrl = "https://api.binance.com/"
        self.requestUrls = []
        self.allData = []
        

    def getAllSymbols(self):                                          
        binance = ccxt.binance()    
        binance_markets = dict()
        for (k,v) in binance.load_markets().items():

            if v["quote"] == "USDT":
                binance_markets[v['id']] = v

        return binance_markets

    def initHistorical(self, symbol):

        interval = "1m"
        limit = 1000
        endTime= int(time.time())*1000
        increment = 60000000
        res = req.get(f'https://api.binance.com/api/v3/klines?symbol={symbol}&interval={interval}&startTime=0')
        epochTime = res.json()[0][0]
        getter = itemgetter(0, 4)
        
        

        #list of all request api
        for startTime in range(epochTime,endTime,increment):
            self.requestUrls.append(f'https://api.binance.com/api/v3/klines?symbol={symbol}&interval={interval}&startTime={startTime}&endTime={endTime}&limit={limit}')

        
        schedule.every().minute.at(":05").do(self._jobs)
        print(self._getTimeNow()+": start")

        while True:
            
            schedule.run_pending()
                 
            if (len(self.requestUrls) == 0):
                schedule.clear()
                break

        self.allData = list(map(list, map(getter, self.allData)))
        df = pd.DataFrame(self.allData, columns=['openTime','close'])
        os.makedirs(f'/mnt/d/DATA/Crypto/binance/{symbol}', exist_ok=True) 
        df.to_csv(f'/mnt/d/DATA/Crypto/binance/{symbol}/1m.csv', index=False)

        print(f'{symbol} complete!')    
       

    def _getTimeNow(self):
        return datetime.now().strftime("%X")

    def _jobs(self):

        if(len(self.requestUrls)>=1000):
            print(self._getTimeNow()+": "+ str(len(self.requestUrls)) + " to fetch")
            print(self._getTimeNow()+": "+ "fetching 1000")
            responses = asyncio.get_event_loop().run_until_complete(fetch_all(self.requestUrls[0:1000], http_get))

            for res in responses:
                for dataPoint in res:

                    self.allData.append(dataPoint)
            del self.requestUrls[:1000]
            print(self._getTimeNow()+": "+ str(responses[0][0][0]) +" to "+ str(responses[-1][-1][0]) )
            print(self._getTimeNow()+": "+ str(len(self.requestUrls)) + " to fetch")
        else:
            print(self._getTimeNow()+": "+ str(len(self.requestUrls)) + " to fetch")
            print(self._getTimeNow()+": "+ "fetching remaining")
            responses = asyncio.get_event_loop().run_until_complete(fetch_all(self.requestUrls, http_get))
            for res in responses:
                for dataPoint in res:
                    self.allData.append(dataPoint)
            self.requestUrls = []
            print(self._getTimeNow()+": "+ str(responses[0][0][0]) +" to "+ str(responses[-1][-1][0]) )
            print(self._getTimeNow()+": " + "done")


def main():
    binance = Binance()
    binance.initHistorical("BTCUSDT")

if __name__ == "__main__":
    main()




    