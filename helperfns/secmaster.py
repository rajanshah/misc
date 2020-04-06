import logging

from helpers.connections import get_mysql_connection

def get_fx_pairs(config):
	''' get fx pairs '''

	query = ''' SELECT symbol, alternate
				FROM fx_pairs '''

	conn = get_mysql_connection(config)
	rs = None
	try:
		with conn.cursor() as cursor:
			cursor.execute(query)
			rs = cursor.fetchall()
	except Exception as ex:
		logging.error('get_fx_pairs error - %s' % str(ex.args))

	rows = {}
	for rec in rs:
		rows[rec[0]] = rec[1]

	return rows



def get_etf_universe(config):
	''' get etf symbols '''

	query = ''' SELECT symbol FROM etf_universe '''
	conn = get_mysql_connection(config)
	rs = None
	try:
		with conn.cursor() as cursor:
			cursor.execute(query)
			rs = cursor.fetchall()
	except Exception as ex:
		logging.error('get_etf_universe - %s' % str(ex.args))

	securities = []
	for rec in rs:
		securities.append(rec[0])

	return securities


def get_security_universe(config, index_name):
	''' get security universe '''

	query = ''' SELECT symbol
				FROM index_universe 
				WHERE index_name = '%s' ''' % (index_name)

	conn = get_mysql_connection(config)
	rs = None
	try:
		with conn.cursor() as cursor:
			cursor.execute(query)
			rs = cursor.fetchall()
	except Exception as ex:
		logging.error('get_security_universe - %s' % str(ex.args))

	securities = []
	for rec in rs:
		securities.append(rec[0])

	return securities

def get_security_universe_combined(config):
	''' get security universe combined '''

	query = ''' SELECT distinct symbol
				FROM index_universe 
				WHERE index_name  in (\'SP500\', \'NDX100\', \'DJI\') '''

	conn = get_mysql_connection(config)
	rs = None
	try:
		with conn.cursor() as cursor:
			cursor.execute(query)
			rs = cursor.fetchall()
	except Exception as ex:
		logging.error('get_security_universe_combined - %s' % str(ex.args))

	securities = []
	for rec in rs:
		securities.append(rec[0])

	return securities

def get_index_universe(config):
	''' get indices information '''

	query = ''' SELECT ticker FROM indices '''
	conn = get_mysql_connection(config)
	rs = None
	try:
		with conn.cursor() as cursor:
			cursor.execute(query)
			rs = cursor.fetchall()
	except Exception as ex:
		logging.error('get_indices error - %s' % str(ex.args))

	rows = []
	for rec in rs:
		rows.append(rec[0])

	return rows


def get_index_universe_alt(config):
	''' get indices information '''

	query = ''' SELECT alternate, ticker FROM indices '''
	conn = get_mysql_connection(config)
	rs = None
	try:
		with conn.cursor() as cursor:
			cursor.execute(query)
			rs = cursor.fetchall()
	except Exception as ex:
		logging.error('get_indices error - %s' % str(ex.args))

	rows = {}
	for rec in rs:
		rows[rec[0]] = rec[1]

	return rows

def get_index_universe_google(config):
	''' get indices information '''

	query = ''' SELECT index_ticker FROM indices '''
	conn = get_mysql_connection(config)
	rs = None
	try:
		with conn.cursor() as cursor:
			cursor.execute(query)
			rs = cursor.fetchall()
	except Exception as ex:
		logging.error('get_index_universe_google error - %s' % str(ex.args))

	rows = []
	for rec in rs:
		rows.append(rec[0])

	return rows

def get_security_universe_with_exch(config):
	''' get security universe '''

	query = ''' SELECT distinct symbol, `name` as company, exchange
				FROM securitymaster sp
				WHERE exchange in ('NYSE', 'NASDAQ')
				AND index_name  in ('$SPX', '$DOWC', '$DOWI', '$DOWT', '$DOWU', '$IQY') '''

	conn = get_mysql_connection(config)
	rs = None
	try:
		with conn.cursor() as cursor:
			cursor.execute(query)
			rs = cursor.fetchall()
	except Exception as ex:
		logging.error('get_security_universe_with_exch error - %s' % str(ex.args))

	rows = {}
	for rec in rs:
		rows[rec[0]] =  {
			'company': rec[1],
			'exchange': rec[2]
		}

	return rows

def get_security_universe_all(config):
	''' get security universe '''

	query = ''' SELECT symbol, `name` as company FROM securitymaster
				WHERE exchange in ('NYSE', 'NASDAQ', 'AMEX') '''
	conn = get_mysql_connection(config)
	rs = None
	try:
		with conn.cursor() as cursor:
			cursor.execute(query)
			rs = cursor.fetchall()
	except Exception as ex:
		logging.error('get_security_universe_all error - %s' % str(ex.args))

	rows = {}
	for rec in rs:
		rows[rec[0]] = rec[1]

	return rows

def get_universe_by_index(config, index_name):
	''' get security universe '''

	query = ''' SELECT symbol
				FROM securitymaster
				WHERE index_name = \'%s\' ''' % (index_name)
	conn = get_mysql_connection(config)
	rs = None
	try:
		with conn.cursor() as cursor:
			cursor.execute(query)
			rs = cursor.fetchall()
	except Exception as ex:
		logging.error('get_universe_by_index - %s' % str(ex.args))

	rows = []
	for rec in rs:
		rows.append(rec[0])

	return rows
