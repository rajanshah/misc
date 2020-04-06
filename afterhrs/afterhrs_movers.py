import argparse
import re
import rethinkdb as r
import pytz
import requests
import time

from bs4 import BeautifulSoup

from datetime import datetime

from dateutil.parser import parse

from helpers.config import read_config
from helpers.connections import get_rethink_connection
from helpers.exception import exception_handler
from helpers.icalendar import get_today
from helpers.log import get_logger

from urllib.request import urlopen
from urllib.error import URLError

logger = get_logger()

TABLE_AFTERHRS='aftermarket_movers'

def parse_arguments():
	''' parse arguments '''

	parser = argparse.ArgumentParser(description='After Hours Downloader')
	parser.add_argument('--config', action='store', dest='config', \
		required=True, help='config')
	result = parser.parse_args()
	return result

@exception_handler(return_if_exception=[])
def parse_after_hours_movers(div_table):
	''' parse_after_hours_movers '''

	records = []
	eastern = pytz.timezone('US/Eastern')
	rows = div_table.find_all('tr')
	dataset = parse_rows(rows)
		
	for key, val in dataset.items():

		if key == 0:
			continue

		symbol = val.get('Symbol', '').strip()
		company = val.get('Name', '').strip()
		last_sale = val.get('Last Sale', '').strip()			
		after_hour_last_sale = val.get('After HoursLast Sale', '').strip()
		after_hour_time = val.get('After Hours Time', '').strip()
		percent_change = val.get('After Hours%Change', '').strip()
		move = val.get('class_move')
				
		percentage_change = 0.0
		if '%' in percent_change :
			percentage, direction = percent_change.split('%', 1)
			percentage_change = percentage.strip()

		try:
			percentage_change = float(percentage_change)
			last_sale = float(last_sale) if last_sale else ''
			after_hour_last_sale = float(after_hour_last_sale) \
				if after_hour_last_sale else ''

		except Exception as ex:
			logger.error('Exception in converting float {}'.format(ex))
			
		process_time = ''
		new_after_hour_time = ''
		try:
			process_time = eastern.localize(datetime.now())
			after_hour_time = parse(after_hour_time)
			new_after_hour_time = eastern.localize(after_hour_time)
		
		except Exception as ex:
			logger.error('Exception in time convert {}'.format(ex))

		
		record = {
			'symbol' : symbol,
			'company' : company,
			'last_sale' : last_sale,
			'after_hour_last_sale' : after_hour_last_sale,
			'last_sale_unit' : 'Dollars',
			'move' : move,
			'change_percent' : percentage_change,
			'process_time' : process_time,
			'after_hour_time' : new_after_hour_time,
			'id' : '{}-{}'.format(symbol, str(new_after_hour_time))
		}
		records.append(record)
	return records		
		
@exception_handler(return_if_exception={})		
def parse_rows(rows):
	''' parse rows '''
	
	date_expr = re.compile('[0-9]{0,2}/[0-9]{0,2}/[0-9]{0,4}')
	dataset = {}
	headers = []
	
	for row_id, row in enumerate(rows):
		cells = row.find_all("th") if row_id == 0 else row.find_all("td")
		dataset[row_id] = {}

		for cell_id, cell in enumerate(cells):

			if row_id == 0:
				temp_text = cell.get_text(strip=True).strip('\n')
				temp_text = date_expr.sub('', temp_text)
				headers.append(temp_text.strip())
			else:
				cell_text = cell.get_text(strip=True).strip('\n')
				cell_text = cell_text.replace(u'\xa0', u' ')
				dataset[row_id][headers[cell_id]] = cell_text
								
			dataset[row_id]['class_move'] = ''
			
			if row.find("span", attrs={'class' : 'green'}):
				dataset[row_id]['class_move'] = 'up'
			
			if row.find("span", attrs={'class' : 'red'}):
				dataset[row_id]['class_move'] = 'down'
							
	return dataset

@exception_handler(return_if_exception=[])
def fetch_after_hours_movers(after_hour_url):
	''' fetch after hours movers '''
	
	headers = {'User-Agent': 'Mozilla/5.0'}

	records = []
	while True:
		logger.info('Processing url: {}'.format(after_hour_url))
		session = requests.Session()
		html = session.get(after_hour_url, headers=headers).text
		soup = BeautifulSoup(html, features='lxml')
		div_table = soup.find('div', attrs={'class' : 'genTable'})

		if not div_table:
			time.sleep(10)
			continue
		
		records = parse_after_hours_movers(div_table)
		if len(records) == 0:
			time.sleep(10)
			continue
		else:
			break
		
	return records

	
if __name__ == '__main__':
	''' 
		pre-market run
		python after_market.py --config=/home/ubuntu/datafeed.cfg
	'''

	# parse arguments
	results = parse_arguments()

	# get configuration
	config = read_config(results.config)

	# Logger setup
	logger = get_logger(app_name='datafeed::after_hours', config=config)

	after_hour_url = config.get('MARKET_MOVERS', 'AFTER_HOURS')
	records = fetch_after_hours_movers(after_hour_url)

	conn = get_rethink_connection(config)
	try:
		result = r.table(TABLE_AFTERHRS)\
			.insert(records, conflict="replace").run(conn)
		
		logger.info('Total inserted : {}'.format(result['inserted']))

	except Exception as ex:
		logger.error('Exception in inserting db database {0}'.format(ex))
