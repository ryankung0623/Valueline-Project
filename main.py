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
from services.writer import *
from itertools import cycle

pd.options.mode.chained_assignment = None

TICKERS = pd.read_csv(config['TICKERS_CSV_PATH'], dtype = str)
RAW_DATA_DIR = config['RAW_DATA_PATH']
EXCHANGE_CODE = config['EXCHANGE_CODE']
OUTPUT_PATH = config['OUTPUT_PATH']


TICKERS = list(TICKERS.iloc[:,0])
TICKERS = TICKERS[2500:]

thread_list = []



workers = 5
# cycle cannot be turned into list, need to create a separate reference for quiting Crawlers later
crawlers_list = [Crawler() for _ in range(workers)]
crawlers = cycle(crawlers_list)

for index, ticker in enumerate(tqdm(TICKERS)):
    while True:
        current_crawler = next(crawlers)
        if current_crawler.is_available():
            t = threading.Thread(target = current_crawler.download, args=(ticker, EXCHANGE_CODE, RAW_DATA_DIR))
            t.start()
            thread_list.append(t)
            time.sleep(0.05)
            break

            
for thread in thread_list:
    thread.join()

for crawler in crawlers_list:
    crawler.quit()

print('Finished downloading raw data...')



#get all tickers
TICKERS = os.listdir(RAW_DATA_DIR)

try:
    TICKERS.remove('.DS_Store')
except Exception as e:
    print(e)
TICKERS.sort()

#get all sectors
sectors = dict()
for ticker in TICKERS:
    data = load_pickle(RAW_DATA_DIR + ticker)
    
    if data["Sector"] not in sectors.keys():
        sectors[data["Sector"]] = [ticker]
    else:
        sectors[data["Sector"]].append(ticker)

for sector in sectors.keys():
    print(f'Compiling {sector} sector tearsheet...')
    #open a doc
    document = Document()
    # trim the margins down to 1cm on each side
    sections = document.sections
    for section in sections:
        section.top_margin = Cm(1)
        section.bottom_margin = Cm(1)
        section.left_margin = Cm(1)
        section.right_margin = Cm(1)

    i = 1
    #compose tear sheets sector by sector
    for ticker in tqdm(sectors[sector]):
        i += 1
        
        try:
            data = load_pickle(RAW_DATA_DIR + ticker)

            #if the company doesn't belong to sector go to next ticker
            if data["Sector"] != sector:
                continue

            document.add_heading(ticker, 0)

            p = document.add_paragraph()

            meta = ['Market Cap', 'Sector', 'Industry' , 'Beta', 'Price/Sales', 'Consensus Forward P/E', 'Price/Book']
            for each in meta:
                if each not in data.keys():
                    continue
                s = p.add_run("%s  -  %s\t\t"%(each,data[each]))
                s.font.name = 'Arial'
                s.font.size = Pt(8)

            s = p.add_run("\n\n")

            s = p.add_run(data['company description'])
            s.font.name = 'Arial'
            s.font.size = Pt(8)

            part1 = pd.concat([add_header(data['quick financials']),data['profitability ratio']])
            df2table(document, part1)

            p = document.add_paragraph()

            #part2 = pd.concat([add_header(data['balance sheet']),data['liquidity']])
            df2table(document, add_header(data['balance sheet']))
            df2table(document, add_header(data['liquidity']))

            df2table(document, add_header(df_filter(data['efficiency'],['Days Sales Outstanding','Days Inventory','Payables Period','Cash Conversion Cycle'])))
        except Exception as e:
            print(e)
        document.add_page_break()
        
    try:
        document.save(f'{OUTPUT_PATH}{sector}.docx')
    except FileNotFoundError:
        with open(f'{OUTPUT_PATH}{sector}.docx', 'w') as f:
            # just create a file if not found and don't do anything to it
            pass
        document.save(f'{OUTPUT_PATH}{sector}.docx')
        


# #convert all docs to pdfs
# to_pdf(PDF_OUTPUT_PATH)
