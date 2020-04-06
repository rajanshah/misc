import argparse
import re
import pytz
import requests
import rethinkdb as r
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


def parse_arguments():
	''' parse arguments '''

	parser = argparse.ArgumentParser(description='Pre Market Hours Downloader')
	parser.add_argument('--config', action='store', dest='config', \
		required=True, help='config')
	result = parser.parse_args()
	return result

@exception_handler(return_if_exception=[])
def parse_pre_hours_movers(div_table):
	''' parse_pre_hours_movers '''

	records = []
	eastern = pytz.timezone('US/Eastern')
	rows = div_table.find_all('tr')
	dataset = parse_rows(rows)
								
	change_map = {
		'\u00a0': ' ',
		'\u25b2': 'up',
		'\u25bc': 'down'
	}

	for key, val in dataset.items():

		if key == 0:
			continue

		symbol = val.get('Symbol', '').strip()
		company = val.get('Name', '').strip()
		last_sale = val.get('Last Sale', '').strip()			
		pre_market_last_sale = val.get('Pre-MarketLast Sale', '').strip()
		pre_market_time = val.get('Pre-Market Time', '').strip()
		percent_change = val.get('Pre-Market %Change', '').strip()
	
		percentage_change = 0.0
		move = ''
		if '%' in percent_change :
			percentage, direction = percent_change.split('%', 1)
			percentage_change = percentage.strip()
			move = change_map.get(direction)

		try:
			percentage_change = float(percentage_change)
			last_sale = float(last_sale) if last_sale else ''
			pre_market_last_sale = float(pre_market_last_sale) \
				if pre_market_last_sale else ''

		except Exception as ex:
			logger.error('Exception in converting float {}'\
				.format(ex))
	
		process_time = ''
		new_pre_market_time = ''
		try:
			process_time = eastern.localize(datetime.now())
			pre_market_time = parse(pre_market_time)
			new_pre_market_time = eastern.localize(pre_market_time)
		
		except Exception as ex:
			logger.error('Exception in time convert {}'.format(ex))

		
		record = {
			'symbol' : symbol,
			'company' : company,
			'last_sale' : last_sale,
			'last_sale_unit' : 'Dollars',
			'move' : move,
			'change_percent' : percentage_change,
			'process_time' : process_time,
			'pre_market_time' : new_pre_market_time,
			'pre_market_last_sale' : pre_market_last_sale,
			'id' : '{}-{}'.format(symbol, str(process_time.date()))
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
		cells = row.find_all("th") if row_id == 0 \
			else row.find_all("td")

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
							
	return dataset

@exception_handler(return_if_exception=[])
def fetch_pre_hours_movers(pre_market_url):
	''' fetch pre hours movers '''

	headers = {'User-Agent': 'Mozilla/5.0'}
	
	records = []
	while True:
		logger.info('Processing url: {}'.format(pre_market_url))
		session = requests.Session()
		html = session.get(pre_market_url, headers=headers).text
		soup = BeautifulSoup(html, features='lxml')
		div_table = soup.find('div', attrs={'class' : 'genTable'})

		if not div_table:
			time.sleep(10)
			continue
		
		records = parse_pre_hours_movers(div_table)
		if len(records) == 0:
			time.sleep(10)
			continue
		else:
			break
		
	return records

	
if __name__ == '__main__':
	''' 
		python pre_market_movers.py --config=/home/ubuntu/datafeed.cfg
	'''

	# parse arguments
	results = parse_arguments()

	# get configuration
	config = read_config(results.config)

	# Logger setup
	logger = get_logger(app_name='datafeed::pre_market_hours', config=config)

	pre_market_url = config.get('MARKET_MOVERS', 'PREMKT_HOURS')
	records = fetch_pre_hours_movers(pre_market_url)

	conn = get_rethink_connection(config)
	try:
		result = r.table('premarket_movers')\
			.insert(records, conflict="replace").run(conn)
		
		logger.info('Total inserted : {}'.format(result['inserted']))

	except Exception as ex:
		logger.error('Exception in inserting db database {0}'.format(ex))
