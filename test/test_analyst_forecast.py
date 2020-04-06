import configparser
import os
import pytz
import sys
import unittest

from datetime import datetime

from mock import patch, MagicMock

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../forecast')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../factordb')))

from factor_scores import get_browser
from analyst_forecast import get_records_from_rows
from analyst_forecast import retrieve_analyst_forecast
from analyst_forecast import save_results_data
from analyst_forecast import scrape_analyst_forecast_change

from selenium.webdriver.remote.webelement import WebElement

class AnalystForecastTest(unittest.TestCase):

	@patch('analyst_forecast.webdriver.remote.webdriver.WebDriver')
	@patch('analyst_forecast.webdriver.Firefox')
	def test_get_browser(self, chrome, driver):
		''' To test get_browser '''

		config = configparser.ConfigParser()
		config['FIREFOX'] = {}
		config['FIREFOX']['DRIVER_PATH'] = 'driver_path'		
		chrome.return_value = driver
		self.assertEquals(get_browser(config), driver)

	@patch('analyst_forecast.get_rethink_connection')
	@patch('analyst_forecast.rethink')
	def test_save_results_data(self, conn, rethink):
		''' To test save_results_data '''

		save_results_data({'data' : 'abc'}, 'config')

	def test_get_records_from_rows(self):
		''' To test get_records_from_rows '''
		
		eastern = pytz.timezone('US/Eastern')
		run_date = eastern.localize(datetime.strptime('2019-01-02', '%Y-%m-%d'))

		headers = MagicMock(WebElement)
		headers.find_elements_by_tag_name.return_value = [
			MagicMock(WebElement, text='Symbol'),
			MagicMock(WebElement, text='Name'),
			MagicMock(WebElement, text='Market'),
			MagicMock(WebElement, text='Current'),
			MagicMock(WebElement, text='1week ago'),
			MagicMock(WebElement, text='change'),
			MagicMock(WebElement, text='total'),
			MagicMock(WebElement, text='up'),
			MagicMock(WebElement, text='down'),
		]
		row_value_list = [
			MagicMock(WebElement, text='SYM'),
			MagicMock(WebElement, text='SYM market association inc'),
			MagicMock(WebElement, text='MARKET-G'),
			MagicMock(WebElement, text='-0.27'),
			MagicMock(WebElement, text='-0.28'),
			MagicMock(WebElement, text='0.04'),
			MagicMock(WebElement, text='1'),
			MagicMock(WebElement, text='1'),
			MagicMock(WebElement, text='0'),			
		]
		first_row = MagicMock(WebElement)
		first_row.find_elements_by_tag_name.return_value = row_value_list
		rows = [
			headers,
			first_row,
			first_row,
			first_row
		]
		output = get_records_from_rows(rows, 'type', 'tab', run_date)
		self.assertEquals(len(output), 2)
		self.assertEquals(output[0].get('id'), 'SYM-2019-01-02-type-tab')
	
	@patch('analyst_forecast.time.sleep')
	@patch('analyst_forecast.save_results_data')
	@patch('analyst_forecast.scrape_analyst_forecast_change')
	@patch('analyst_forecast.webdriver.remote.webdriver.WebDriver')
	def test_retrieve_analyst_forecast(self, browser, scrape, save, sleep):
		''' To test retrieve_analyst_forecast '''
		
		data = [{
			'scrapped_data' : 'srapped---data'
		}]
		scrape.return_value = data
		retrieve_analyst_forecast('earnings_{}_url_{}', browser, \
			'2019-03-12', 'config')
		self.assertEquals(scrape.call_count, 8)
		save.assert_called_with(data, 'config')
	
	@patch('analyst_forecast.time.sleep')
	@patch('analyst_forecast.get_records_from_rows')
	@patch('analyst_forecast.webdriver.remote.webdriver.WebDriver')
	def test_scrape_analyst_forecast_change(self, browser, records, sleep):
		''' To test scrape_analyst_forecast_change '''
	
		records.return_value = [
			{
				'Symbol' : 'ABCDE',
			},
			{
				'Symbol' : 'PQRST',
			}
		]
		table = MagicMock(WebElement)
		row = MagicMock(WebElement)
		row.find_element_by_partial_link_text.side_effect = [
			MagicMock(WebElement),
		]
		table.find_elements_by_tag_name.return_value = [
			row,
		]
		browser.find_element_by_css_selector.return_value = table

		output = scrape_analyst_forecast_change(browser, 'type', 'tab', 'date')
		self.assertEquals(len(output), 4)
		self.assertEquals(browser.execute_script.call_count, 1)
	
