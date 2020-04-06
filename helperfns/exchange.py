import os
import csv

def get_exchange_mappings(exchanges, config):
	''' read universe '''
	
	securities = {}
	for exchange in exchanges:
		exchange_universe = '%s_%s' % (exchange, 'UNIVERSE')
		universe_path = config.get('BARRONS_FUNDAMENTALS', exchange_universe.upper())

		with open(universe_path) as csvfile:
			reader = csv.DictReader(csvfile)
			for row in reader:
				securities[row['Symbol']] = exchange

	return securities
