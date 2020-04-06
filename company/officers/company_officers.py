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
	parser = argparse.ArgumentParser(description='Google Finance')
	parser.add_argument('--root_dir', action='store', dest='root_dir',\
		required=True, help='root directory')
	parser.add_argument('--config', action='store', dest='config',\
		required=True, help='config file')
	result = parser.parse_args()
	return result

def clean_text(text):
	'''
		clean the text scraped by scraper 
	'''
	text = "".join(word for word in text.get_attribute('textContent') if ord(word) < 128)\
		.replace("\n","").replace("\t","").strip()
	return text

def scrape_officer_data(config, url_page, root_dir, firefox):
	'''
		scraper for capitalideas using geckodriver
		creates json dump
	'''

	browser = webdriver.Firefox(executable_path=firefox)

	total_officers = 0
		
	# fetch data company wise
	companies = get_security_universe_with_exch(config)
	for company in companies.keys():
		logging.info('Processing security %s' % company)
		browser.get(url_page + company)
		
		# start scrapping
		table = browser.find_elements_by_class_name('dataTable')
		trs = table[0].find_elements_by_tag_name('tr')
		for tr in trs:
			dict = {}
			tds = tr.find_elements_by_tag_name('td')
			if tds == []:
				continue
			dict['name'] = tds[0].text
			dict['age'] = 0
			try:
				dict['age'] = clean_text(tds[1])
			except Exception as ex:
				if tds[1].find(',') > 0:
					dict['age'] = clean_text(tds[1].replace(',', ''))

			if dict['age'] != '':
				dict['age'] = int(dict['age'])
			dict['since'] = clean_text(tds[2])
			dict['position'] = clean_text(tds[3])
			if dict['position'].find(',') > 0:
				dict['position'] = dict['position'].split(',')
			print dict['name']

			# summary data for officer
			trs_officers = table[1].find_elements_by_tag_name('tr')
			for tr_officer in trs_officers:
				tds_officer = tr_officer.find_elements_by_tag_name('td')
				if tds_officer == []:
					continue
				if tds_officer[0].text == dict['name']:
					dict['summary'] = clean_text(tds_officer[1])
					break

			# create a json sump
			officers_path = os.path.join(root_dir,'officers','%s' % company)
			if not os.path.exists(officers_path):
				os.makedirs(officers_path)

			total_officers = total_officers + 1
			with open("%s/%s.json" % (officers_path, dict['name']),'w') as fp:
				json.dump(dict,fp)

	return total_officers


if __name__ == '__main__':
	'''
		python company_officers.py --root_dir=/var/feed/data/ --config=/home/ubuntu/datafeed.cfg
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
	officers = scrape_officer_data(config, url_page, root_dir, firefox)
	logging.info('Total officers %s' % officers)
