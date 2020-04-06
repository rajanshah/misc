import configparser
import mock
import os
import sys
import unittest

from datetime import date, datetime

from mock import patch, MagicMock

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../barrons')))

from barrons_stories import click_button
from barrons_stories import prepare_state
from barrons_stories import process_request
from barrons_stories import save_results
from barrons_stories import scrape_records
from barrons_stories import process_and_save_picks

from selenium.webdriver.remote.webelement import WebElement
from selenium.common.exceptions import StaleElementReferenceException

class BarronStoriesTest(unittest.TestCase):

	@classmethod
	def setUpClass(self):
		pass
	
	@patch('barrons_stories.webdriver.remote.webdriver.WebDriver')
	@patch('barrons_stories.webdriver.remote.webelement.WebElement')
	def test_click_button(self, element, browser):
		''' To test click_button '''
		
		browser.find_elements_by_xpath.return_value = [element, element]
		click_button(browser, 'button_text')
		browser.find_elements_by_xpath.assert_called_with(\
			"//button[contains(., 'button_text')]")
		element.click.assert_called_once()

	@patch('barrons_stories.click_button')
	@patch('barrons_stories.time.sleep')
	@patch('barrons_stories.webdriver.remote.webdriver.WebDriver')
	@patch('barrons_stories.webdriver.remote.webelement.WebElement')
	def test_prepare_state(self, element, browser, sleep, click):
		''' To test prepare_state '''

		element.text = 'ed'
		browser.find_elements_by_xpath.return_value = [element, element]
		prepare_state(browser, 'edition', 'ed_type')
		click.assert_called_with(browser, 'Apply Filter')
		self.assertEquals(click.call_count, 5)

	@patch('barrons_stories.prepare_state')
	@patch('barrons_stories.scrape_records')
	@patch('barrons_stories.click_button')
	@patch('barrons_stories.time.sleep')
	@patch('barrons_stories.webdriver.remote.webdriver.WebDriver')
	@patch('barrons_stories.webdriver.remote.webelement.WebElement')
	@patch('barrons_stories.webdriver.Chrome')
	@patch('barrons_stories.Select')
	def test_process_request(self, select, chrome, element, browser, sleep, \
		click, scrape, prepare):
		''' To test process_request '''

		select.return_value = select
		chrome.return_value = browser
		scrape.return_value = []
		config = configparser.ConfigParser()
		config['CHROME'] = {}
		config['CHROME']['DRIVER_PATH'] = 'driver_path'
		config['BARRONS_STORIES'] = {}
		config['BARRONS_STORIES']['STOCK_PICK_URL'] = 'stock_url'
		process_date = date(2019, 1, 3)	
		element.text = 'element'
		browser.find_element_by_xpath.return_value = select
		browser.find_elements_by_xpath.return_value = [element, element]
		
		output = process_request(config, process_date, 'WEEKLY')
		click.assert_called_with(browser, 'element')
		select.select_by_visible_text.assert_called_with('2019')
		self.assertEquals(len(output), 6)
		output = process_request(config, 2018, 'YEARLY')
		select.select_by_visible_text.assert_called_with('2018')
	
	@patch('barrons_stories.process_request')
	@patch('barrons_stories.save_results')
	def test_process_and_save_picks(self, save, process):
		''' To test set_mode '''
		
		process_and_save_picks('config_db', 'date', 0)
		process.assert_called_with('config_db', 'date', 'WEEKLY')

		process_and_save_picks('config_db', 'date', 2018)
		process.assert_called_with('config_db', 2018, 'YEARLY')

	@patch('barrons_stories.rethink')
	@patch('barrons_stories.get_rethink_connection')
	def test_save_results(self, conn, rethink):
		''' To test save_results '''
		resultset = [
			{
				'type' : 'type',
				'category' : 'cat',
				'resultset':[{
					'issue_date' : 'Mar 21 2019',
					'symbol' : 'SYM',
					'benchmark' : 'BM',
					'company' : 'Company'
				}],
			},
		]
		save_results('config', resultset)
		
	@patch('barrons_stories.WebDriverWait')
	@patch('barrons_stories.time.sleep')
	@patch('barrons_stories.webdriver.remote.webdriver.WebDriver')
	def test_scrape_records(self, browser, sleep, wait):
		''' To test scrape_records '''
		
		wait.until.return_value = True
		
		table_elm = MagicMock(WebElement)
		browser.find_element_by_tag_name.return_value = table_elm
		line = MagicMock(WebElement)
		
		table_elm.find_elements_by_tag_name.return_value = [line, line]
		element_mock = MagicMock(WebElement)
		
		index_dict = {
			'S&P 500' : 'SPX',
		}
		
		item = MagicMock(WebElement)
		item.find_elements_by_tag_name.return_value = [item, item, item]
		item.get_attribute.return_value = '12.3'
		
		item7 = MagicMock(WebElement)
		item7.get_attribute.return_value = '0.93'		
		
		item8 = MagicMock(WebElement)
		item8.get_attribute.return_value = 'S&P 500'		

		items = [
			MagicMock(WebElement),
			MagicMock(WebElement, text='SYM'),
			MagicMock(WebElement, text='My SYM company inc'),
			MagicMock(WebElement, text='03/06/2019'),
			item,
			item,
			MagicMock(WebElement, text='14.2'),
			item7,
			item8
		]
		line.find_elements_by_tag_name.return_value = items
		process_date = datetime(2019, 3, 9)
		
		browser.find_elements_by_css_selector.return_value = [
			MagicMock(WebElement)
		]
		output = scrape_records(browser, process_date, 'WEEKLY', index_dict, \
			1, [], [])
		self.assertEquals(len(output), 2)
		browser.find_elements_by_css_selector.return_value = [
			MagicMock(WebElement),
		]*2
		output = scrape_records(browser, process_date, 'WEEKLY', index_dict, \
			1, [], [])
		self.assertEquals(len(output), 4)

