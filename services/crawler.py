from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
import numpy as np
import pickle
import os

import time



class Crawler:
        def __init__(self):
                ##driver config
                PATH = "/Applications/chromedriver"
                chrome_options = Options()  
                chrome_options.add_argument("window-size=1200x600")
                #chrome_options.add_argument("user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_5) AppleWebKit/605.1.15 (KHTML, like Gecko) CriOS/85 Version/11.1.1 Safari/605.1.15")
                ##Switching headless option on and off
                headless = True

                if headless:
                        chrome_options.add_argument("--headless")
                        chrome_options.add_argument('disable-gpu')

                self.driver = webdriver.Chrome(executable_path=ChromeDriverManager().install(), chrome_options=chrome_options)
                ##driver.set_window_position(0, 0)
                ##driver.set_window_size(1400, 1024)

        def download(self, ticker, exchange_code, raw_data_dir) -> str:
                '''use get request to obtain financial data corresponding to the ticker and exchange
                return success or error message

                after fetching the data, write data to raw_data_dir
                
                return data/error, message
                '''
                try:
                        raw_data = os.listdir(raw_data_dir)
                except FileNotFoundError:
                        os.mkdir(raw_data_dir)
                        raw_data = os.listdir(raw_data_dir)

                
                # if ticker file already exist in raw-data directory, don't fetch data for second time
                if ticker in raw_data:
                        print(ticker + " done...")
                        return

                else:
                        result, msg = self.get(ticker, exchange_code)
                        if msg == 'success':
                                self.write(result, f'{raw_data_dir}/{ticker}')
                                print(ticker + " done...")
                        else:
                                print(result)
                        return

        def get(self, ticker="", exchange="") -> str:
                '''use get request to obtain financial data corresponding to the ticker and exchange
                return success or error message
                the actual data will be stored in the object's attribute, data 
                
                return data/error, message
                '''

                code = ticker
                if exchange != "":
                        code = exchange + ":" + code

                # dictionary of data
                data = {}


                self.driver.get("https://financials.morningstar.com/ratios/r.html?t=%s"%(code))


                ##get to financial

                try:
                        #quick financials
                        wait = WebDriverWait(self.driver, 10).until(EC.visibility_of_all_elements_located((By.XPATH, '//*[@id="financials"]/table')))
                        wait.clear()
                        table = self.driver.find_element_by_xpath('//*[@id="financials"]/table')
                        table = pd.read_html(table.get_attribute("outerHTML"))[0]
                        table = table[table.iloc[:,0].notna()]
                        table.rename(columns={ table.columns[0]: "" }, inplace = True)
                        data["quick financials"] =  table

                        time.sleep(1)

                        #income statement
                        table = self.driver.find_element_by_xpath('//*[@id="tab-profitability"]/table[1]')
                        table = pd.read_html(table.get_attribute("outerHTML"))[0]
                        table = table[table.iloc[:,0].notna()]
                        table.rename(columns={ table.columns[0]: "" }, inplace = True)
                        data["income statement"] =  table

                        #profitability ratio
                        table = self.driver.find_element_by_xpath('//*[@id="tab-profitability"]/table[2]')
                        table = pd.read_html(table.get_attribute("outerHTML"))[0]
                        table = table[table.iloc[:,0].notna()]
                        table.rename(columns={ table.columns[0]: "" }, inplace = True)
                        data["profitability ratio"] =  table

                        tab = self.driver.find_element_by_partial_link_text("Financial Health")
                        tab.click()

                        #balance sheet
                        table = self.driver.find_element_by_xpath('//*[@id="tab-financial"]/table[1]')
                        table = pd.read_html(table.get_attribute("outerHTML"))[0]
                        table = table[table.iloc[:,0].notna()]
                        table.rename(columns={ table.columns[0]: "" }, inplace = True)
                        data["balance sheet"] =  table

                        #liquidity
                        table = self.driver.find_element_by_xpath('//*[@id="tab-financial"]/table[2]')
                        table = pd.read_html(table.get_attribute("outerHTML"))[0]
                        table = table[table.iloc[:,0].notna()]
                        table.rename(columns={ table.columns[0]: "" }, inplace = True)
                        data["liquidity"] =  table

                        tab = self.driver.find_element_by_partial_link_text("Efficiency Ratio")
                        tab.click()

                        #efficiency
                        table = self.driver.find_element_by_xpath('//*[@id="tab-efficiency"]/table')
                        table = pd.read_html(table.get_attribute("outerHTML"))[0]
                        table = table[table.iloc[:,0].notna()]
                        table.rename(columns={ table.columns[0]: "" }, inplace = True)
                        data["efficiency"] =  table

                        quote_page = self.driver.find_element_by_partial_link_text("Quote")
                        quote_page.click()


                        #company description
                        wait = WebDriverWait(self.driver, 10).until(EC.visibility_of_all_elements_located((By.CLASS_NAME, 'stock__profile-description-text')))
                        wait.clear()
                        comp_desc = self.driver.find_element_by_class_name('stock__profile-description-text').text
                        data["company description"] =  comp_desc

                        #check if all data are loaded on the page
                        for i in range(10):
                                wait = WebDriverWait(self.driver, 10).until(EC.presence_of_all_elements_located((By.CLASS_NAME, 'snap-panel')))
                                elements = self.driver.find_elements_by_class_name('snap-panel')
                                texts = [e.text for e in elements if e.text != '']
                                fully_loaded = all([True if "\n" in text else False for text in texts])
                                if fully_loaded:
                                        break
                                
                                time.sleep(1)


                        #Market Cap
                        #Beta
                        #Price/Sales
                        #Consensus Forward P/E
                        #Price/Book
                        data_name = ["Market Cap","Beta","Price/Sales","Consensus Forward P/E","Price/Book"]
                        for text in texts:
                                for name in data_name:
                                        if name in text:
                                                data[name] = text.split("\n")[1]


                        #stock__content-articles stock__profile
                        profile = self.driver.find_element_by_class_name('stock__profile-items').text
                        profile = profile.split("\n")
                        data["Sector"] = profile[profile.index("Sector") + 1]
                        data["Industry"] = profile[profile.index("Industry") + 1]

                        return data, 'success'


                except Exception as e:
                        return e, 'error'

        def quit(self):
                self.driver.quit()

        def write(self, data, file_path):
                """pickle data to file_path"""
                with open(file_path,"wb") as file:
                        pickle.dump(data, file)
                        

        def read(self, file_path):
                """read and return pickled data from file_path to dictionary"""
                with open(file_path,"rb") as file:
                        return pickle.load(file)

        