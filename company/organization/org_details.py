import argparse
import json
import logging
import os
import requests
import sys
import time
from selenium import webdriver
from selenium.webdriver.support.ui import Select

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils import read_config
from utils.secmaster import get_security_universe_with_exch

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)


def parse_arguments():
	''' parse arguments '''

	# parse arguments
	parser = argparse.ArgumentParser(description='Organization Details')
	parser.add_argument('--root_dir', action='store', dest='root_dir',\
		required=True, help='root directory')
	parser.add_argument('--config', action='store', dest='config',\
		required=True, help='config file')
	result = parser.parse_args()
	return result

def scrape_organization_details(config, url_page, root_dir, firefox):
	'''
		scraper for capitalideas using geckodriver
		creates json dump
	'''

	browser = webdriver.Firefox(executable_path=firefox)
	browser.get(url_page)

	# pre process : set United States 
	settings = browser.find_element_by_partial_link_text('Settings')
	settings.click()
	search = browser.find_element_by_partial_link_text('Search settings')
	search.click()
	time.sleep(2)

	showmore = browser.find_element_by_id('regionanchormore')
	showmore.location_once_scrolled_into_view
	showmore.click()

	US = browser.find_element_by_xpath("//div[@id='regionoUS']")
	US.click()
	save = browser.find_element_by_xpath\
		("//div[@class='goog-inline-block jfk-button jfk-button-action']")
	save.click()
	alert = browser.switch_to.alert
	alert.accept()
	time.sleep(3)

	# keys to be stored as a list
	keylist = ['Products', 'Subsidiaries', 'TV shows', 'Founders', 'Executives']
		
	# fetch data company wise
	companies = get_security_universe_with_exch(config)
	data_dir = os.path.join(root_dir, 'organization')
	if not os.path.exists(data_dir):
		os.makedirs(data_dir)

	for company, details in companies:
		exch = details['exchange']
		logging.info('Processing security %s' % company)
		
		search = browser.find_element_by_xpath("//input[@title='Search']")
		search.clear()
		search.send_keys("%s: "+ (exch, company))
		button = browser.find_element_by_xpath("//button[@name='btnG']")
		button.click()		
		dict={}
		finance = browser.find_element_by_xpath("//div[@data-tab-id='COMPANY']")
		finance.click()
		time.sleep(3)

		data = browser.find_element_by_xpath("//div[@class='_f2t _Q0t']")
		summary = data.find_element_by_tag_name('span').text
		dict['summary'] = summary
		records = data.find_elements_by_class_name('mod')
		for count in range(1,len(records)):
			columns=records[count].find_elements_by_tag_name('span')
			# click for more
			if(len(columns) >= 3):
				if(columns[2].text == ', MORE'):
					columns[2].click()
			_key = columns[0].text.replace(':','')
			if(_key in keylist):
				_val = columns[1].text.split(',')
			else:
				_val = columns[1].text
			dict[_key] = _val
  
		# create a json dump
		filepath = os.path.join(data_dir, '%s.json' % company)
		with open(filepath,'w') as fp:
			json.dump(dict,fp)

	browser.quit()
	return total_count


if __name__ == '__main__':
	'''
		python subsidiaries.py --root_dir=/var/feed/data/ --config=/home/ubuntu/datafeed.cfg
	'''
	
	# parse arguments
	result = parse_arguments()

	# get root directory info
	root_dir = result.root_dir

	# read configuration info
	config = read_config(result.config)

	url_page = config.get('REUTERS','OFFICER_URL')
	firefox =  config.get('FIREFOX','DRIVER_PATH')
	
	# scrape data for officers
	officers = scrape_organization_details(config, url_page, root_dir, firefox)
	logging.info('Total officers %s' % officers)
