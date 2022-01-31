from services.crawler import Crawler
from helper.config import config
import pandas as pd
import os





tickers = pd.read_csv(config['TICKERS_CSV_PATH'])
tickers = list(tickers.iloc[:,0])


raw_data_dir = os.listdir(config['RAW_DATA_PATH'])

crawler = Crawler()

for ticker in tickers:
        if ticker in raw_data_dir:
                print(str(tickers.index(ticker)+1) + ". " + ticker + " done...")
                continue

        msg = crawler.get(ticker)
        if msg == 'success':
            crawler.write(raw_data_dir + ticker)
            print(str(tickers.index(ticker)+1) + ". " + ticker + " done...")
        else:
            print(msg)
            continue

crawler.quit()