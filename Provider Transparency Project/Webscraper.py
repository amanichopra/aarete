import sys

# only needed if running on Aarete terminal server
sys.path.append('c:/program files/python38/lib/site-packages')

from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import Select
import pandas as pd
import csv
import time
from datetime import datetime
import logging
import numpy as np
import threading
from tqdm import tqdm
import argparse
from sqlalchemy import create_engine
import urllib

def get_TQ_prov_info(driver, wait_time):
    """
    Fetches name and address from TQH for provider.

    :param driver: selenium webdriver element; driver navigated to provider page on TQ health
    :param wait_time: int; number seconds to wait before looking for elements on page

    :return: list containing name and address

    """
    logger.debug('Finding information tabs for provider. Common options are "Provider" and "Disclaimer".')
    info_tabs = driver.find_element_by_xpath('//div[contains(@class, "more-information-tabs")]').find_element_by_tag_name('ul').find_elements_by_tag_name('li')
    ind = None
    for i in range(len(info_tabs)):
        if info_tabs[i].text == 'Provider':
            ind = i
            break
    logger.debug(f'Found index of "Provider tab at index {ind}."')

    info_tabs[i].click()
    time.sleep(wait_time)
    logger.debug('Fetching provider name...')
    name = driver.find_element_by_xpath('//div[contains(@class, "hospital-info-title")]').text
    logger.debug('Fetching provider address...')
    address = driver.find_element_by_xpath('//div[contains(@class, "hospital-detail-map")]').text.split('\n')[1]
    return [name] + [address]

def get_comparison_stats(driver):
    """
    Fetches comparison states from TQH for praticular service.

    :param driver: selenium webdriver element; driver navigated to provider page on TQ health
    
    :return: dict with keys containing statistic name and values containing statistic value as proportion
    """
    stats = {}
    logger.debug('Fetching comparison statistics...')
    price_comp_stats = driver.find_element_by_xpath('//div[contains(@class, "hospital-pricing-comparison")]').find_elements_by_tag_name('ul')
    for stat in price_comp_stats:
        stats[stat.find_element_by_tag_name('h3').text] = stat.find_element_by_tag_name('h4').text
    return stats
    
def get_cash_price(driver):
    """
    Fetches cash price from TQH for particular serivce.

    :param driver: selenium webdriver element; driver navigated to provider page on TQ health
    
    :return: str containing cash price
    """    
    logger.debug('Fetching cash price...')
    return driver.find_element_by_xpath('//div[contains(@class, "cost-estimation-calculator")]').find_element_by_id('cashPriceAmount').text
    
def get_insurance_price(driver, wait_time):
    """
    Fetches insurance prices from TQH for particular service.

    :param driver: selenium webdriver element; driver navigated to provider service page on TQ health
    :param wait_time: int; number seconds to wait before looking for elements on page 
    
    :return: dict with keys containing insurance name and values containing price
    """
    prices = {}
    try:
        driver.find_element_by_xpath('//div[contains(@class, "payment-select-button")]').click() # click insurance price button
        time.sleep(wait_time)
    except Exception:
        logger.warning('Insurance prices not available! Expect missing value in insurance price field.')
        return None
    
    ins_plans = Select(driver.find_element_by_id('insurance-plan-selection')).options[1:-1]
    for plan in ins_plans:
        plan_name = plan.text
        if plan_name in prices:
            continue
            
        logger.debug(f'Fetching insurance price for {plan_name}...')
        plan.click() # select plan in dropdown
        time.sleep(wait_time / 2)
        try:
            prices[plan_name] = driver.find_element_by_xpath('//div[contains(@class, "estimation-calculator")]').find_element_by_xpath('//*[@id="app"]/div[1]/div[1]/div[2]/div/div[1]/ul/li[1]/span').text
        except Exception:
            logger.warning(f'Insurance price not available for {plan_name}!')
            
    return prices

def process_data(all_scrape_data):
    """
    Processes data in format needed to write to SQL

    :param all_scrape_data: DataFrame containing all data scraped
    
    :return: Processed DataFrame containing all data scraped
    """
    # drop records w/ no provider info
    all_scrape_data = all_scrape_data[~all_scrape_data['Provider'].isna()].reset_index(drop=True)

    # drop if scraping using state
#     if 'TQH Name' in all_scrape_data.columns and 'TQH Address' in all_scrape_data.columns:
#         all_scrape_data = all_scrape_data.drop(columns=['TQH Name', 'TQH Address'])
    
    all_scrape_data[['Code Type', 'Code Value']] = pd.DataFrame(all_scrape_data['CPT'].str.split(' ').tolist(), columns=['Code Type', 'Code Value'])
    all_scrape_data = all_scrape_data.drop(columns=['CPT'])

    # explode insurance prices
    all_scrape_data['Insurance Price'] = all_scrape_data['Insurance Price'].map(lambda x: list(eval(str(x)).items()) if not pd.isna(x) else x)
    all_scrape_data = all_scrape_data.explode('Insurance Price')
    all_scrape_data['Insurance Price'] = all_scrape_data['Insurance Price'].map(lambda x: list(x) if type(x) != float else x)
    all_scrape_data[['Insurer', 'Insurance Price']] = all_scrape_data['Insurance Price'].apply(pd.Series).rename(columns={0:'Insurer', 1: 'Insurance Price'})

    all_scrape_data = all_scrape_data[['Provider', 'Zip Code', 'Code Type', 'Code Value', 'Service', 'Cash Price', 'Insurer', 'Insurance Price', 'Comparison Statistics']].rename(columns={'Zip Code': 'Zip_CD', 'Code Type': 'Code_Type', 'Code Value': 'Code_Value', 'Cash Price': 'Cash_Price', 'Insurance Price': 'Insurance_Price', 'Comparison Statistics': 'Comparison_Statistics'}) # rename to match output SQL table
    
    all_scrape_data['Scrape_Num'] = float('nan')
    # drop rows missing cash and insurance price
    all_scrape_data = all_scrape_data[~all_scrape_data['Cash_Price'].isna() | ~all_scrape_data['Insurance_Price'].isna()].reset_index(drop=True)

    # format address
    all_scrape_data[['Address', 'City', 'State', 'Zip_CD']] = all_scrape_data['Zip_CD'].str.split(', ', expand=True)

    return all_scrape_data
    
def get_data_for_provider(driver, prov_name=None, prov_zip_code=None, search_url=None, page_load_wait=1, element_visible_wait=3, max_pages=float('inf'), max_services=float('inf'), output_csv_path=None, return_df=False):
    """
    Fetches all data for provider

    :param prov_name: str name of provider to search on TQ Health
    :param prov_zip_code: str zip code of provider to search on TQ Health
    :param search_url: str specific provider URL to search on TQ Health; use either search_url OR prov_name and prov_zip_code
    :param page_load_wait: int number seconds to wait for page to load
    :param element_visible_wait: int number seconds to wait for elements
    :param max_pages: int max number of pages to scan through for particular provider
    :param max_services: int max number of services to scan through for provider; use either max_pages or max_services for best performance (helpful for debug)
    :param output_csv_path: str path to write data to as csv file
    :param return_df: bool specifying whether or not to return data as DataFrame
    
    :return: Processed DataFrame containing all data scraped
    """
    if not prov_name and not prov_zip_code and not search_url: raise Exception('Must supply either provider name and zip to search, or a URL!')
    if prov_name and prov_zip_code: logger.info(f'Fetching data for provider: {prov_name} with zipcode: {prov_zip_code}')
    
    i = 1
    
    if return_df:
        data = []

    while i <= max_pages:
        if prov_name and prov_zip_code:
            prov_name_url = '-'.join(prov_name.lower().split(' '))
            logger.debug(f'Opening TH URL for provider: {prov_name} (page {i}).')
            driver.get(f'https://turquoise.health/service_offerings?q=&service_name=&location={prov_zip_code}&provider_name={prov_name_url}&page={i}&distance=10')
        elif prov_name:
            prov_name_url = '-'.join(prov_name.lower().split(' '))
            logger.debug(f'Opening TH URL for provider: {prov_name} (page {i}).')
            driver.get(f'https://turquoise.health/service_offerings?q=&service_name=&location={prov_name_url}&provider_name={prov_name_url}&page={i}&distance=10')
        else:
            driver.get(search_url)
            
        time.sleep(page_load_wait)
        
        num_services = driver.find_elements_by_xpath('//div[contains(@class, "service-info-cont")]')
        time.sleep(element_visible_wait)
        num_services = int(min(len(num_services), max_services))
        
        if num_services == 0:
            logger.warning(f'No information available for provider: {prov_name}! Moving to next provider...')
            return None
        
        for j in range(num_services):
            logger.debug(f'Scraping service # {j} on page {i}.')
            logger.debug('Scraping service information...')
            
            try:
                servs = driver.find_elements_by_xpath('//div[contains(@class, "service-info-cont")]')
                time.sleep(element_visible_wait)
                serv_info = servs[j].text.split('\n')
            except Exception:
                logger.warning(f'No service information available for provider: {prov_name}!')
                continue
            
            logger.debug('Scraping rate information...')   
            try:
                rate_btns = driver.find_elements_by_xpath('//a[contains(@class, "rate-button")]')
                time.sleep(element_visible_wait)
                rate_btns[j].click()
                time.sleep(page_load_wait)
            except Exception:
                logger.warning(f'No rates available for provider: {prov_name} for service: {serv_info[1]}! This provider will not be added to output table.')
                continue
            
            try:
                prov_info = get_TQ_prov_info(driver, page_load_wait)
            except Exception:
                logger.warning(f'Could not fetch provider info for provider: {prov_name} for service: {serv_info[1]}!')
                prov_info = [None]
                
                
            try:
                comp_stats = get_comparison_stats(driver)
            except Exception:
                logger.warning(f'Could not fetch comparison stats for provider: {prov_name} for service: {serv_info[1]}!')
                comp_stats = None
            
            try:
                cash_price = get_cash_price(driver)
            except Exception:
                logger.warning(f'Could not fetch cash price for provider: {prov_name} for service: {serv_info[1]}!')
                cash_price = None
            
            try:
                ins_price = get_insurance_price(driver, page_load_wait)
            except Exception:
                logger.warning(f'Could not fetch insurance price for provider: {prov_name} for service: {serv_info[1]}!')
                ins_price = None
            
            driver.back()
            
            if prov_name and prov_zip_code:
                row = [prov_name, prov_zip_code] + serv_info[:2] + [cash_price, ins_price, comp_stats] + prov_info
            elif prov_name:
                row = prov_info + serv_info[:2] + [cash_price, ins_price, comp_stats]
            else:
                row = serv_info[:2] + [cash_price, ins_price, comp_stats] + prov_info
            
            logger.debug('Appening row to table...')
            if output_csv_path: 
                 with open(output_csv_path, 'a') as f:
                    writer = csv.writer(f)
                    writer.writerow(row)
            if return_df: data.append(row)

            time.sleep(page_load_wait + 5)

        i += 1
        
    if return_df:
        if prov_name and prov_zip_code:
            prov_data = pd.DataFrame(data, columns=['Provider', 'Zip Code', 'CPT', 'Service', 'Cash Price', 'Insurance Price', 'Comparison Statistics', 'TQH Name', 'TQH Address'])
        elif prov_name:
            prov_data = pd.DataFrame(data, columns=['Provider', 'Zip Code', 'CPT', 'Service', 'Cash Price', 'Insurance Price', 'Comparison Statistics'])
        else:
            prov_data = pd.DataFrame(data, columns=['CPT', 'Service', 'Cash Price', 'Insurance Price', 'Comparison Statistics', 'TQH Name', 'TQH Address'])
        return prov_data
    

def get_provs_df_for_state(state, driver_options, max_pages=float('inf')):
  """
    Fetches names of providers in particular state

    :param state: str abbreviation of state search on TQ Health
    :param driver_options: list of options to use for selenium driver

    :return: DataFrame containing names of providers in state
    """
    # webdriver options
    chrome_options = Options()
    for option in driver_options:
        chrome_options.add_argument(f'--{option}')
        
    # create driver
    driver = webdriver.Chrome(ChromeDriverManager().install(),options=chrome_options)
    
    prov_df = []
    
    i = 1
    while i <= max_pages:
        driver.get(f'https://turquoise.health/providers?state={state}&page={i}&letter=')
        try:
            prov_names = driver.find_element_by_class_name('three-columns').find_elements_by_tag_name('li')
        except Exception:
            break
            
        for prov in prov_names:
            prov_df.append(prov.text)
        
        i += 1
    driver.quit()
    return pd.DataFrame(prov_df, columns=['NAME'])
        
    
    

def scrape_provs(driver_options, provs_search_df, log_file_path='webscraper_progress.log', page_load_wait=1.5, element_visible_wait=2, max_pages=float('inf'), max_services=float('inf'), output_csv_path=None, return_df=True, write_sql=True, aarete_user='achopra', max_hospitals=float('inf'), randomize_scrape=False):
    """
    Main function to scrape pricing data for providers

    :param driver_options: list of options to use for selenium driver
    :param provs_search_df: DataFrame containing names of providers to scrape pricing info for
    :param log_file_path: str path specifying logging destination
    :param page_load_wait: int number seconds to wait for page to load
    :param element_visible_wait: int number seconds to wait for elements
    :param max_pages: int max number of pages to scan through for particular provider
    :param max_services: int max number of services to scan through for provider; use either max_pages or max_services for best performance (helpful for debug)
    :param output_csv_path: str path to write data to as csv file
    :param return_df: bool specifying whether or not to return data as DataFrame
    :write_sql: bool specifying whether or not to write data to SQL table
    :aarete_user: str AARETE userid to post in SQL table; only needed if writing to SQL
    :max_hospitals: int max number of providers to fetch pricing info for
    :randomize_scrape: bool specifying whether or not to scrape data for hospitals in random order
    
    """
    # webdriver options
    chrome_options = Options()
    for option in driver_options:
        chrome_options.add_argument(f'--{option}')
        
    # create driver
    driver = webdriver.Chrome(ChromeDriverManager().install(),options=chrome_options)
    
    # logging config
    global logger
    logger = logging.getLogger('webscraper')
    logger.setLevel(logging.DEBUG)
    if log_file_path:
        fh = logging.FileHandler(log_file_path, mode='w')
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logger.addHandler(fh)
    else:
        logger.setLevel(logging.CRITICAL)
    
    
    pricing_df = pd.DataFrame()
    
    if output_csv_path:
        with open(output_csv_path, 'w') as f:
            writer = csv.writer(f)
            if 'ZIP' in provs_search_df.columns:
                writer.writerow(['Provider', 'Zip Code', 'CPT', 'Service', 'Cash Price', 'Insurance Price', 'Comparison Statistics', 'TQH Name', 'TQH Address'])
            else:
                writer.writerow(['Provider', 'Zip Code', 'CPT', 'Service', 'Cash Price', 'Insurance Price', 'Comparison Statistics'])
    
    max_hospitals = int(min(max_hospitals, provs_search_df.shape[0]))
    if randomize_scrape: provs_search_df = provs_search_df.sample(frac=1).reset_index(drop=True)
    for ind, row in tqdm(provs_search_df.iloc[:max_hospitals].iterrows(), total=max_hospitals):
        name = row['NAME']
        try:
            zip_c = row['ZIP']
        except KeyError:
            zip_c = None
        prov_data = get_data_for_provider(driver, prov_name=name, prov_zip_code=zip_c, page_load_wait=page_load_wait, element_visible_wait=element_visible_wait, output_csv_path=output_csv_path, max_pages=max_pages, max_services=max_services, return_df=True)
   
        pricing_df = pd.concat([pricing_df, prov_data]).reset_index(drop=True)
    
    driver.quit()
    
    if write_sql:
        pricing_df = process_data(pricing_df)
        
        # add info to SQL table
        pricing_df['SOURCE_FILE_NAME'] = float('nan')
        pricing_df['LOAD_DT'] = datetime.now()
        pricing_df['LOAD_BY'] = f'AARETE\{aarete_user}\webscraper.py'
        pricing_df['REQUESTED_BY'] = f'AARETE\{aarete_user}'
        pricing_df['UNIQUE_KEY'] = float('nan')
        pricing_df['AUDIT_KEY'] = float('nan')
        
        conn_str = (r'Driver={SQL Server};'
                      r'Server=AARSVRSQL;'
                      r'Database=REFERENCE;'
                      r'Trusted_Connection=yes;')
        quoted_conn_str = urllib.parse.quote_plus(conn_str)
        conn = create_engine(f'mssql+pyodbc:///?odbc_connect={quoted_conn_str}').connect()
        
        pricing_df.astype(str).to_sql('RPT.HOSPITAL_PRICE_TRANSPARENCY', conn, if_exists='append', index=False)

    if return_df: return pricing_df
    
if __name__ == '__main__':
    driver_options = ['incognito', 'window-size=1920,1080', 'disable-gpu', 'disable-extensions', "proxy-server='direct://'", 'proxy-bypass-list=*', 'start-maximized', 'no-sandbox', 'headless']

    parser = argparse.ArgumentParser()
    parser.add_argument('--output_file_path', default=None) # path to save log & csv files to
    parser.add_argument('--num_threads', type=int, default=5) # number of webdriver instances to run on different threads
    parser.add_argument('--state', default=None) # abbreviation of state
    parser.add_argument('--prov_df_path', default=None) # path to data with providers to search for
    parser.add_argument('--driver_options', type=list, default=driver_options) # webdriver options
    parser.add_argument('--write_sql', type=bool, default=False) # boolean indicating whether or not to write data to sql table
    parser.add_argument('--element_visible_wait', type=int, default=0) # num seconds to wait to find elements on page
    parser.add_argument('--page_load_wait', type=int, default=2) # num seconds to wait for page to load
    parser.add_argument('--max_pages', type=float, default=float('inf')) # max num pages to iterate through/provider
    parser.add_argument('--max_services', type=float, default=float('inf')) # max num services to iterate through/provider
    parser.add_argument('--aarete_user', type=str, default='achopra') # user requesting service
    parser.add_argument('--max_hospitals', type=float, default=float('inf')) # max num hospitals to scrape
    parser.add_argument('--randomize_scrape', type=bool, default=False) # bool indicating whether or not to randomize which hospitals to scrape

    args = parser.parse_args()
    if args.state and args.prov_df_path:
        raise Exception('Must supply either state or prov_df_path, not both!')
       
    elif not args.state and args.prov_df_path:
        raise Exception('Must supply either state or prov_df_path. None passed as arguments!')
    elif args.prov_df_path:
        prov_df_path = args.prov_df_path
        provs_df = pd.read_csv(fr'{prov_df_path}')
    else:
        provs_df = get_provs_df_for_state(args.state, args.driver_options, max_pages=args.max_pages)
   
    provs = list(provs_df['NAME'].values)
    print(f'Scraping data for the following providers: {provs}')

    # threading support to parallelize scrapes
    threads = []
    for i, provs_df_sub in enumerate(np.array_split(provs_df, args.num_threads)):
        out_path = args.output_file_path
        if out_path:
            p = threading.Thread(target=scrape_provs, args=[args.driver_options, provs_df_sub], kwargs={'output_csv_path': fr'{out_path}\prov_{i+1}_data.csv', 'element_visible_wait': args.element_visible_wait, 'page_load_wait': args.page_load_wait, 'log_file_path': fr'{out_path}\webscraper_progress_{i + 1}.log', 'write_sql': args.write_sql, 'aarete_user': args.aarete_user, 'max_hospitals': args.max_hospitals, 'max_pages': args.max_pages, 'max_services': args.max_services, 'randomize_scrape': args.randomize_scrape})
        else:
            p = threading.Thread(target=scrape_provs, args=[args.driver_options, provs_df_sub], kwargs={'output_csv_path': None, 'element_visible_wait': args.element_visible_wait, 'page_load_wait': args.page_load_wait, 'log_file_path': None, 'write_sql': args.write_sql, 'aarete_user': args.aarete_user, 'max_hospitals': args.max_hospitals, 'max_pages': args.max_pages, 'max_services': args.max_services, 'randomize_scrape': args.randomize_scrape})
        p.start()
        threads.append(p)
        for thread in threads:
            thread.join()
        
        logging.shutdown()
