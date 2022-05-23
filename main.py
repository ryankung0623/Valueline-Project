from http.server import ThreadingHTTPServer
from multiprocessing.pool import ThreadPool
from services.crawler import Crawler
from helper.config import config
import pandas as pd
import os
from concurrent.futures import ThreadPoolExecutor
import threading
from tqdm import tqdm
import time

tickers = pd.read_csv(f"tickers/{config['TICKERS_CSV_PATH']}")
raw_data_dir = f"raw-data/{config['RAW_DATA_PATH']}"
exchange_code = config['EXCHANGE_CODE']


tickers = list(tickers.iloc[:,0])

tickers = tickers[2035:]

thread_list = []
crawlers = []


workers = 30

for _ in range(workers):
    crawlers.append(Crawler())

for index, ticker in enumerate(tqdm(tickers)):
    t = threading.Thread(target = crawlers[index%workers].download, args=(ticker, exchange_code, raw_data_dir))
    t.start()
    thread_list.append(t)

    if len(thread_list) == workers:
        for t in thread_list:
            t.join()
        thread_list = []
        time.sleep(1)

print('reach') 
# with ThreadPoolExecutor(max_workers=4) as executor:
#     for ticker in tqdm(tickers):
#         future = executor.submit(crawler.download, ticker, exchange_code, raw_data_dir)



