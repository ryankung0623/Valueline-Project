from helper.config import config
from helper.picky_numbers import picky_mean, convert_to_float

from services.crawler import Crawler
import services.writer as writer

import threading
from tqdm import tqdm
import pandas as pd

import os
import time
import re
from itertools import cycle

# turn off pandas warning
pd.options.mode.chained_assignment = None

# loading env variables
MODE = int(config['MODE'])
RAW_DATA_DIR = config['RAW_DATA_PATH']
EXCHANGE_CODE = config['EXCHANGE_CODE']
OUTPUT_PATH = config['OUTPUT_PATH']
NUMBER_OF_THREADS = int(config['NUMBER_OF_THREADS'])
TICKERS_COLUMN = int(config['TICKERS_COLUMN'])

tickers = pd.read_csv(config['TICKERS_CSV_PATH'], dtype = str)
tickers = list(tickers.iloc[:,TICKERS_COLUMN-1])

if MODE in [0,2]:

    thread_list = []
    workers = NUMBER_OF_THREADS
    # cycle cannot be turned into list, need to create a separate reference for quiting Crawlers later
    crawlers_list = [Crawler() for _ in range(workers)]
    crawlers = cycle(crawlers_list)

    for ticker in tqdm(tickers):
        match = re.match(r'([\w\d]+)', ticker)
        ticker = match.group(0)
        while True:
            current_crawler = next(crawlers)
            if current_crawler.is_available():
                t = threading.Thread(target=current_crawler.download, args=(ticker, EXCHANGE_CODE, RAW_DATA_DIR), daemon=True)
                t.start()
                thread_list.append(t)
                time.sleep(0.01)
                break

    # wait until all threads are done
    for thread in thread_list:
        thread.join()

    for crawler in crawlers_list:
        crawler.quit()

    print('Finished downloading raw data...')


## Compiling downloaded raw data into tearsheets

if MODE in [1,2]:
    tickers = os.listdir(RAW_DATA_DIR)

    try:
        tickers.remove('.DS_Store')
    except Exception as e:
        print(e)
    tickers.sort()

    # get all sector names and append tickers to the right sector
    sectors = dict()
    for ticker in tickers:
        data = writer.load_pickle(RAW_DATA_DIR + ticker)
        
        if data["Sector"] not in sectors.keys():
            sectors[data["Sector"]] = [ticker]
        else:
            sectors[data["Sector"]].append(ticker)

    for sector in sectors.keys():
        print(f'Compiling {sector} sector tear sheets...')
        #open a doc
        document = writer.Document()
        # trim the margins down to 1cm on each side
        sections = document.sections
        for section in sections:
            section.top_margin = writer.Cm(1)
            section.bottom_margin = writer.Cm(1)
            section.left_margin = writer.Cm(1)
            section.right_margin = writer.Cm(1)

        #compose tear sheets sector by sector
        for ticker in tqdm(sectors[sector]):
            try:
                data = writer.load_pickle(RAW_DATA_DIR + ticker)
                document.add_heading(ticker, 0)
                p = document.add_paragraph()

                # Indirect method of calculating PE ratio
                profitability_ratio_table = data['profitability ratio'].set_index('')
                last_3y_margins = list(profitability_ratio_table.loc['Net Margin %'])[-3:]
                average_margin = picky_mean(last_3y_margins)
                average_margin_decimal = average_margin/100
                PS = convert_to_float(data['Price/Sales'])
                PE = PS/average_margin_decimal
                data['P/E'] = round(PE,2)

                # composing quick summary
                quick_info = ['Market Cap', 'Sector', 'Industry' , 'P/E', 'Price/Sales', 'Price/Book']
                for info in quick_info:
                    if info not in data.keys():
                        continue

                    s = p.add_run(f"{info}: {data[info]}        ")
                    s.font.name = 'Arial'
                    s.font.size = writer.Pt(8)

                # empty lines between quick summary and detail information
                s = p.add_run("\n\n")

                s = p.add_run(data['company description'])
                s.font.name = 'Arial'
                s.font.size = writer.Pt(8)

                part1 = pd.concat([writer.add_header(data['quick financials']),data['profitability ratio']])
                writer.df2table(document, part1)

                p = document.add_paragraph()

                #part2 = pd.concat([writer.add_header(data['balance sheet']),data['liquidity']])
                writer.df2table(document, writer.add_header(data['balance sheet']))
                writer.df2table(document, writer.add_header(data['liquidity']))

                writer.df2table(document, writer.add_header(writer.df_filter(data['efficiency'],['Days Sales Outstanding','Days Inventory','Payables Period','Cash Conversion Cycle'])))

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
            
    #convert all docs to pdfs
    # writer.to_pdf(OUTPUT_PATH)

