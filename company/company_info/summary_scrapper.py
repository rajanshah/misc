import json
import logging
import os
import requests
import time
from selenium import webdriver
from selenium.webdriver.support.ui import Select

# keys to be stored as a list
keylist = ['Products', 'Subsidiaries', 'TV shows', 'Founders', 'Executives']

logger = logging.getLogger('intellimind')

def scrape_articles(run_date, url_page, root_dir, firefox, securities):
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

	# dir generation
	dir_path = os.path.join(root_dir, run_date)
	if not os.path.exists(dir_path):
		os.makedirs(dir_path)

	# Starting Scrape
	for company, company_data in securities.iteritems():
		file_path = '%s/%s.json' % (dir_path, company)
		if os.path.exists(file_path):
			logger.info('pass file %s' % file_path)
			continue

		search = browser.find_element_by_xpath("//input[@title='Search']")
		search.clear()
		search_key = "%s: %s" % (company_data['exchange'], company)

		try:
			search.send_keys(search_key)
			logger.info('processing %s' % search_key)
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
	
 	 
			# create a json sump
			with open(file_path, 'w') as fp:
				json.dump(dict, fp)
		except Exception as ex:
			logger.error('Exception in scrapping name %s:%s' % (company, str(ex.args)))	
	browser.quit()

if __name__ == '__main__':

	scrape_articles("https://www.google.com/search?tbm=fin&q=NASDAQ:+AAPL",\
			"/var/feed/data/articles/","/home/khushboo/geckodriver")
