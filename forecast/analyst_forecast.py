import argparse
import pytz
import rethinkdb as rethink
import string
import sys
import time

from datetime import datetime

from helpers.config import read_config
from helpers.connections import get_rethink_connection
from helpers.exception import exception_handler
from helpers.icalendar import get_today
from helpers.log import get_logger

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException

logger = get_logger()

ANALYST_FORECASTS='analyst_forecasts'

def parse_arguments():
	''' parse arguments '''

	# parse arguments
	parser = argparse.ArgumentParser(description='Analyst Forecast')
	parser.add_argument('--config', action='store', dest='config',\
		required=True, help='config')
	parser.add_argument('--date', action='store', dest='date',\
		required=False, help='process_date')
	result = parser.parse_args()
	return result

@exception_handler(return_if_exception=[])
def get_records_from_rows(rows, report_type, report_tab, run_date):
	''' get records from table '''

	records = []
	tb_keys = []
	for tb_key in rows[0].find_elements_by_tag_name("th"):
		tb_keys.append(tb_key.text.replace(' ', ''))

	tb_keys_len = len(tb_keys)
	for row in rows[1:-1]:

		record = {}			
		elems = row.find_elements_by_tag_name("td")
		
		if tb_keys_len != len(elems):
			continue
		
		for indx, elm in enumerate(elems):
			record[tb_keys[indx]] = elm.text.encode('ascii', \
				'ignore').decode()
		
		record_id = '{}-{}-{}-{}'.format(record['Symbol'], \
			str(run_date.date()), report_type, report_tab)
		record_update = {
			'period' : report_tab,
			'type' : report_type,
			'date' : run_date,
			'id' : record_id,
			'symbol' : record.pop('Symbol'),
			'name' : record.pop('Name'),
			'current' : record.pop('Current'),
		}
		record.update(record_update)
		records.append(record)
	
	return records

@exception_handler(return_if_exception=[])
def scrape_analyst_forecast_change(browser, report_type, report_tab, run_date):
	''' scrape_analyst_forecast_change '''
	
	new_records = []
	page = 0
	while True:
		page = page + 1
		logger.info('Processing Page : {}'.format(page))
		
		rows = []
		try:
			table = browser.find_element_by_css_selector(\
				"table[id='two_column_main_content_InfoResults']")
			rows = table.find_elements_by_tag_name("tr")

		except StaleElementReferenceException as ex:
			logger.error('No table found {}'.format(ex))
			break
		
		except TimeoutException as ex:
			logger.error('Timeout in finding table {}'.format(ex))
			break

		records = get_records_from_rows(rows, report_type, report_tab, \
			run_date)

		logger.info('Records fetched : {}'.format(len(records)))
		new_records.extend(records)

		next_elm = None
		try:
			next_elm = rows[-1].find_element_by_partial_link_text('Next')
			
		except Exception as ex:
			logger.info('Completed for {} {}'\
				.format(report_type, report_tab))
			logger.info('No more pages available {}'.format(ex))
			break

		if next_elm:
			try:
				browser.execute_script("arguments[0].click();", next_elm)
				time.sleep(20)
				
			except TimeoutException:
				browser.execute_script("window.stop();")
				time.sleep(10)

	return new_records

@exception_handler(return_if_exception=None)
def retrieve_analyst_forecast(earnings_forecast_url, browser, run_date, config):
	''' retrieve analyst forecast url '''

	report_types = ['INCREASE', 'DECREASE']
	report_tab_nos = {
		1 : 'Current Fiscal Quarter',
		2 : 'Next Fiscal Quarter', 
		11 : 'Current Fiscal Year',
		12 : 'Next Fiscal Year'
	}

	eastern = pytz.timezone('US/Eastern')
	run_date = eastern.localize(datetime.strptime(run_date, '%Y-%m-%d'))
	
	for report_type in report_types:
		for report_tab_no in report_tab_nos:

			report_tab = report_tab_nos[report_tab_no]
			logger.info('report type : {}'.format(report_type))				
			logger.info('report tab : {}'.format(report_tab))
		
			data = []
			try:
				browser.get(earnings_forecast_url.format(report_type, \
					report_tab_no))
				time.sleep(20)
				
			except TimeoutException:
				browser.execute_script("window.stop();")
				time.sleep(10)
			
			data = scrape_analyst_forecast_change(browser, report_type, \
				report_tab, run_date)

			if len(data) != 0:
				save_results_data(data, config)

	if browser:
		browser.quit()

@exception_handler(return_if_exception=None)
def save_results_data(data, config):
	''' save_results_data '''
	
	r_conn = get_rethink_connection(config)
	result = rethink.table(ANALYST_FORECASTS).insert(data, conflict="replace")\
		.run(r_conn)
	logger.info('Saved Analyst forecast in database {}'\
		.format(result['inserted']))
		
@exception_handler(return_if_exception=None)
def create_instance(firefox):
	''' create_instance '''

	options = Options()
	options.headless = True
	browser = webdriver.Firefox(options=options, executable_path=firefox)
	browser.set_page_load_timeout(10)
		
	return browser

if __name__=='__main__':

	'''
		python analyst_forecast.py --config=/home/ubuntu/datafeed.cfg
	'''

	results = parse_arguments()
	config = read_config(results.config)
	date = results.date if results.date else str(datetime.now().date())

	# Logger setup
	logger = get_logger(app_name='datafeed::analyst_forecast', config=config)

	logger.info('Run date : {}'.format(date))

	earnigs_forecast_url = config.get('ANALYST_FORECAST', 'CHANGE_URL')

	firefox = config.get('FIREFOX','DRIVER_PATH')
	browser = create_instance(firefox)
	if not browser:
		sys.exit()
	
	retrieve_analyst_forecast(earnigs_forecast_url, browser, date, config)
