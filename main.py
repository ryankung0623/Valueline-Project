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

html_body = ""

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

    for sector in list(sectors.keys())[:1]:
        print(f'Compiling {sector} sector tear sheets...')

        #compose tear sheets sector by sector
        for ticker in tqdm(sectors[sector][:4]):
            try:
                data = writer.load_pickle(RAW_DATA_DIR + ticker)
                
                html_body += f"<h1>{ticker}</h1>\t"
                
                # Indirect method of calculating PE ratio
                profitability_ratio_table = data['profitability ratio'].set_index('')
                last_3y_margins = list(profitability_ratio_table.loc['Net Margin %'])[-3:]
                average_margin = picky_mean(last_3y_margins)
                average_margin_decimal = average_margin/100
                PS = convert_to_float(data['Price/Sales'])
                PE = PS/average_margin_decimal
                data['P/E'] = round(PE,2)

                html_body += "<p>"

                # composing quick summary
                quick_info = ['Market Cap', 'Sector', 'Industry' , 'P/E', 'Price/Sales', 'Price/Book']
                for info in quick_info:
                    if info not in data.keys():
                        continue

                    html_body += f"{info}: {data[info]}        "

                html_body += "</p>"
                # empty lines between quick summary and detail information
                html_body = "<p>" + html_body + data['company description'] + "</p>"

                part1 = pd.concat([writer.add_header(data['quick financials']), data['profitability ratio']])

                html_body = html_body + pd.concat([data['quick financials'],data['profitability ratio']]).to_html(index=False).replace("\n", "")
                html_body += "<p></p>"
                html_body = html_body + data['balance sheet'].to_html(index=False).replace("\n", "")
                html_body += "<p></p>"
                html_body = html_body + data['liquidity'].to_html(index=False).replace("\n", "")
                html_body += "<p></p>"
                html_body = html_body + writer.df_filter(data['efficiency'],['Days Sales Outstanding','Days Inventory','Payables Period','Cash Conversion Cycle']).to_html(index=False).replace("\n", "")
                html_body += "<p></p>"

            except Exception as e:
                print(e)

            html_body += "<hr>"
            html_body += "<p></p>"

        
        with open('test.html', 'w') as f:
            f.write(f"""<!DOCTYPE html>
            <html>
            <body>
            {html_body}
            </body>
            </html>
            """)
        # try:
        #     document.save(f'{OUTPUT_PATH}{sector}.docx')
        # except FileNotFoundError:
        #     with open(f'{OUTPUT_PATH}{sector}.docx', 'w') as f:
        #         # just create a file if not found and don't do anything to it
        #         pass
        #     document.save(f'{OUTPUT_PATH}{sector}.docx')
            
    #convert all docs to pdfs
    # writer.to_pdf(OUTPUT_PATH)

