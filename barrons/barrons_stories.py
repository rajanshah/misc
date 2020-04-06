import argparse
import requests
import logging
import os
import pytz
import re
import rethinkdb as rethink
import sys
import socket
import time

from bdateutil.parser import parse

from copy import deepcopy

from datetime import datetime, timedelta

from logging.handlers import SysLogHandler

from pyvirtualdisplay import Display

from selenium import webdriver
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from helpers.config import read_config
from helpers.connections import get_rethink_connection
from helpers.context_filter import ContextFilter
from helpers.icalendar import get_today
from helpers.log import get_logger

logger = get_logger()

STOCK_PICKS='barrons_picks'


def parse_arguments():
	''' parse arguments '''

	# parse arguments
	parser = argparse.ArgumentParser(description='Barrons Picks')
	parser.add_argument('--config', action='store', dest='config',\
		required=True, help='config')
	parser.add_argument('--date', action='store', dest='date',\
		required=False, help='process_date')
	parser.add_argument('--year', action='store', dest='year',\
		required=False, help='process_year')
	result = parser.parse_args()
	return result

def click_button(browser, button_text):
	''' click_button '''
	
	try:
		button_list = browser.find_elements_by_xpath(\
			"//button[contains(., '"+button_text+"')]")
		if button_list:
			button_list[0].click()
	
	except Exception as ex:
		logger.error('Exception in click_button {}'.format(ex))

def prepare_state(browser, edition, ed_type):
	''' prepare_state '''

	try:
		click_button(browser, edition)
		click_button(browser, ed_type)
		selects = browser.find_elements_by_xpath(\
			"//button[@class='form-control customOption ng-binding is-selected']")
		
		for select in selects:
			if not select.text == edition:
				click_button(browser, select.text)

		click_button(browser, 'Apply Filter')
		time.sleep(4)

	except Exception as ex:
		logger.error('Exception in prepare_state {}'.format(ex))
	
def scrape_records(browser, process_date, MODE, index_dict, record_index, \
	record_indicies=[], records=[]):
	''' scrape_records '''
	
	try:
		try:
			records_bkp = deepcopy(records)
			wait = WebDriverWait(browser, 20)
			wait.until(EC.visibility_of_element_located((By.TAG_NAME, "tbody")))

			browser.implicitly_wait(30)
			table_elm = browser.find_element_by_tag_name('tbody')

			if table_elm:
				table_list = table_elm.find_elements_by_tag_name('tr')

				for line in table_list:
					items = line.find_elements_by_tag_name('td')
					
					record = {'symbol': items[1].text\
						.encode('ascii', 'ignore').decode()}
					record['company'] = items[2].text\
						.encode('ascii', 'ignore').decode()
					record['issue_date'] = items[3].text\
						.encode('ascii', 'ignore').decode()
					
					if MODE == 'WEEKLY':
						if not ((datetime.strptime(record['issue_date'], \
							"%m/%d/%Y")) < process_date and \
							(datetime.strptime(record['issue_date'], \
							"%m/%d/%Y")) >= (process_date - timedelta(days=7))):
							record ={}
							continue
					
					record['pre_story_price'] = items[4]\
						.find_elements_by_tag_name("span")[1]\
						.get_attribute('textContent')
					record['post_story_price'] = items[5]\
						.find_elements_by_tag_name("span")[2]\
						.get_attribute('textContent')
					record['pct_change'] = items[6].text\
						.encode('ascii', 'ignore').decode()
					record['bm_change'] = items[7]\
						.get_attribute('textContent').encode('ascii', 'ignore')\
						.decode()
					
					record['benchmark'] = items[8].get_attribute('textContent')\
						.encode('ascii', 'ignore').decode()
					if record['benchmark'] in index_dict: 
						record['benchmark'] = index_dict[record['benchmark']]
					
					try:
						if float(record['pre_story_price']) > 0 and \
							float(record['post_story_price']) > 0:
							records.append(record)
					
					except Exception as ex:
						logger.error('Exception : {}'.format(ex))
			
				if record_index == 1:
					record_indicies = browser\
						.find_elements_by_css_selector(\
						"a[class='ng-binding ng-scope']")
				
				record_index += 1
				if len(record_indicies) < record_index:
					return records

				record_indicies[record_index-1].click()
				
				scrape_records(browser,process_date, MODE, index_dict, \
					record_index=record_index, \
					record_indicies=record_indicies, records=records)

		except StaleElementReferenceException as ex:
			scrape_records(browser,process_date, MODE, index_dict, \
				record_index=record_index, record_indicies=record_indicies, \
				records=records_bkp)

	except TimeoutException as ex:
		pass

	finally:
		return records
	
	
def process_request(config, process_date, MODE):
	''' process requests '''

	resultset = []
	try:
	# stock pick url
		stock_picker_url = config.get('BARRONS_STORIES', 'STOCK_PICK_URL')

		# basic tags	
		index_dict = {
			'S&P 500' : 'SPX',
			'S&P MidCap 400' : 'AMID',
			'Russell 2000' : 'RUT'
		}
		editions = [
			'ASIA online',
			'US online',
			'US print'
		]
		editions_mapping = {
			'ASIA online': 'asia',
			'US online': 'online',
			'US print': 'magazine'
		}
		ed_types = ['Bullish', 'Bearish']

		if MODE == 'WEEKLY':
			year_range = process_date.year
		else:
			year_range = process_date

		options = webdriver.ChromeOptions()
		options.headless = True
		browser = webdriver.Chrome(executable_path=config.get(\
			'CHROME', 'DRIVER_PATH'), chrome_options = options)
		logger.info('Stock Picker URL {}'.format(stock_picker_url))
		browser.get(stock_picker_url)

		select = Select(browser.find_element_by_xpath(\
			"//select[@name='originTime']"))
		select.select_by_visible_text(str(year_range))
		selects = browser.find_elements_by_xpath(\
			"//button[@class='form-control customOption ng-binding is-selected']")

		for select in selects:
			click_button(browser, select.text)
		
		for edition in editions:
			for ed_type in ed_types:
				prepare_state(browser, edition, ed_type)
				results = scrape_records(browser, process_date, MODE, \
					index_dict, record_index=1, record_indicies=[], records=[])
				resultset.append({'category': ed_type, \
					'type': editions_mapping[edition], 'resultset': results })
		browser.quit()
		
	except Exception as ex:
		logger.error('Exception in process_request {}'.format(ex))
	
	finally:	
		return resultset

def process_and_save_picks(config_db, date, year):
	''' process and save '''

	mode = 'WEEKLY' if year == 0 else 'YEARLY'
	key_field = date if year == 0 else year

	logger.info('Processing for date:{}'.format(key_field))
	resultset = process_request(config_db, key_field, mode)
	total = save_results(config_db, resultset)
	return total



def save_results(config, resultset):
	''' save results to database '''

	r_conn = get_rethink_connection(config)

	eastern = pytz.timezone('US/Eastern')

	total = 0
	for res in resultset:
		for record in res['resultset']:
			try:
				issue_dt = parse(record['issue_date']).date()\
					.strftime('%Y-%m-%d')
				issue_date = eastern.localize(datetime\
					.strptime(issue_dt, "%Y-%m-%d"))
				
				symbol = record['symbol']
				bm = record['benchmark']

				record['id'] = '{}-{}-{}'.format(issue_dt, symbol, bm)
				record['date'] = eastern.localize(datetime.strptime(\
					get_today().strftime('%Y-%m-%d'), "%Y-%m-%d"))
				record['issue_date'] = issue_date
				record['type'] = res['type']
				record['category'] = res['category']
				logger.info('Record {}'.format(record))
			
				if symbol != '{{recommendation.symbol | SymbolWithCountry}}':
					pick_result = rethink.table(STOCK_PICKS)\
						.insert(record, conflict="replace").run(r_conn)

					pick_record_ins = pick_result['inserted']
					total = total + int(pick_record_ins)

					logger.info('Save Pick: Symbol:{} Firm:{} Date:{} Type:{} Category:{}'\
						.format(record['symbol'], record['company'], \
						record['issue_date'],record['type'], record['category']))

			except Exception as ex:
				logger.error('Error in parsing record {}'.format(ex))

	return total

if __name__ == '__main__':

	'''
		python barrons_stories.py --config=/Users/rajanshah/feed_uat.cfg --root-dir=/var/feed/data/barrons
	'''

	# parse arguments
	results = parse_arguments()

	# read config
	config = read_config(results.config)

	# parse date
	date = results.date if results.date else get_today().strftime('%Y-%m-%d')
	year = results.year if results.year else 0

	# Logging configuration
	logger = get_logger(app_name='datafeed::barrons_stories', config=config)

	run_date = datetime.strptime(date,'%Y-%m-%d')
	year = int(year)

	# set mode
	total = process_and_save_picks(config, run_date, year)
	logger.info('Total records inserted: {}:{}'.format(date, total))
