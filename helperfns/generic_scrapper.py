import logging
import requests
import re
import sys
import time

from urllib.request import urlopen
from urllib.error import URLError

from argparse import ArgumentParser

from bs4 import BeautifulSoup

from dateutil.parser import *

from datetime import datetime
from time import mktime

from helpers.icalendar import get_today

logger = logging.getLogger('datafeed')

def parse_rows(rows):
	''' Get data from rows '''

	results = []
	for row in rows:
		table_headers = row.find_all('th')
		if table_headers:
			results.append([headers.get_text() for headers in table_headers])

		table_data = row.find_all('td')
		if table_data:
			results.append([data.get_text() for data in table_data])

	return results

def parse_table(table):
	''' Get data from table '''
	return [
		[cell.get_text().strip() for cell in row.find_all(['th', 'td'])]
			for row in table.find_all('tr')
	]

def fetch_table_data(url, table_id=None):
	''' fetch table data '''

	# Make soup
	try:
		resp = urlopen(url)
	except URLError as e:
		logger.info('An error occured fetching %s \n %s' % (url, e.reason))
		return 1

	soup = BeautifulSoup(resp.read())

	# find table
	table = soup.find('table', id=table_id) if table_id else soup.find('table')

	# parse table data
	table_data = parse_table(table)

	return table_data

def fetch_table_data_advanced(url, table_id=None, lastUpdated=None, totalPages=None):
	''' fetch table data '''

	# Make soup
	try:
		resp = urlopen(url)
	except URLError as e:
		logger.info('An error occured fetching {} \n {}'.format(url, e.reason))
		return (1, 1)

	soup = BeautifulSoup(resp.read())

	# find table
	table = soup.find('table', id=table_id) if table_id else soup.find('table')

	# find last updated
	last_updated_dt = str(get_today())
	try:
		if lastUpdated:
			START_OFFSET = 15
			END_OFFSET = 34
			position = soup.text.find(lastUpdated)
			if position:
				start_pos = position + START_OFFSET
				end_pos = position + END_OFFSET
				result = soup.text[start_pos:end_pos]
				last_updated_dt = str(parse(result.strip()).date())

	except Exception as ex:
		logger.error('Exception in parsing last updated dt {}'.format(ex))

	# parse table data
	table_data = parse_table(table)

	return (last_updated_dt, table_data)

def fetch_header(url, header):
	''' fetch heder data '''

	# Make soup
	try:
		resp = urlopen(url)
	except URLError as e:
		print(('An error occured fetching %s \n %s' % (url, e.reason)))
		return 1

	soup = BeautifulSoup(resp.read())

	# find header
	header_rows = soup.findAll('h2',{'class':'title'})

	for hrow in header_rows:
		if not hrow.text.find('RISK arbitrage') >= 0:
			result = time.strptime(hrow.text, "%A %d %B %Y")
			report_dt = datetime.fromtimestamp(mktime(result))		
			run_dt = report_dt.strftime('%Y-%m-%d')
			return run_dt

	return None

def fetch_all_tables(url):
	''' fetch multiple tables '''

	try:
		resp = urlopen(url)
	except URLError as e:
		logging.info('An error occured fetching %s \n %s' % (url, e.reason))
		return 1

	soup = BeautifulSoup(resp.read())
	multi_tables = soup.find_all('table')

	return multi_tables

def fetch_table_by_class_ex(url, class_name):
	''' fetch table data '''

	# Make soup
	soup = None
	try:
		resp = requests.get(url)
		soup = BeautifulSoup(resp.text)
		
	except URLError as e:
		print(('An error occured fetching %s \n %s' % (url, e.reason)))
		return 1

	# find table by class
	table_data = soup.findAll('table')
	selected_table = None
	for tdata in table_data:
		if tdata.get('class') and tdata.get('class')[0] == class_name:
			selected_table = tdata
			break

	# parse table data
	table_data = parse_table(selected_table)

	return table_data
