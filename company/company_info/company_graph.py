# -*- coding: utf-8 -*-


###################################################################################
#
# DATE:     	01-18-2016
# AUTHOR(s):   	AJAN LAL SHRESTHA
#              	RAJAN SHAH
# PURPOSE:      Collect various company data
#
# LICENSE:  	IntelliMind LLC
#
###################################################################################


import argparse
import datetime
import logging
import luigi
import os
import sys
import socket
import time
import traceback

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

logger = logging.getLogger('intellimind')

from utils import read_csv_file

from utils.secmaster import get_security_universe_all
from utils.JSONWriter import JSONWriter
from utils.MechanizeBrowser import MechanizeBrowser
from CompanyScraper import extract_company_info, get_officers_link

from utils import read_config
from utils.context_filter import ContextFilter

from logging.handlers import SysLogHandler

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addFilter(ContextFilter())


class CompanySpider(MechanizeBrowser):
	'''Spider for Company Information'''

	def __init__(self, base_url, root_dir):
		'''Initialize the mechanize browser, scraper, json writer'''
		
		MechanizeBrowser.__init__(self)
		self.base_url = base_url

	def reset(self):
		pass

	@classmethod
	def get_today(self):
		'''Return today date'''
		
		return datetime.datetime.today()

	def get_company_data(self, security):
		'''Fetch the full information of the company'''

		try:
			url = self.base_url.format(security)
			main_source = self.open_url(url)
			company_link = get_officers_link(main_source, security)
			company_url = "http:{0}".format(company_link) if company_link else ""
			company_source = self.open_url(company_url) if company_url else ""
			return extract_company_info(security, main_source, company_source)
		except Exception, ex:
			logger.error('Error in capturing company info:%s-%s' % (security,str(ex.args)))

		return None

	def save_to_json(self, root_dir, file_name, data):
		'''Save the data as json'''

		json_writer = JSONWriter(root_dir)
		json_writer.write_json_content(file_name, data)

	def extract_company_data(self, sec_universe, root_dir):
		'''Fetch the companies data and save it'''

		success = 0
		failure = 0
		try:
			for security, company in sec_universe.iteritems():
				data = self.get_company_data(security)
				if data:
					file_name = "{0}.json".format(security)
					self.save_to_json(root_dir, file_name, data)
					success += 1
					time.sleep(1.5)
		except Exception, err:
			failure += 1
			logger.warn('Failure for getting data for security %s-%s' % (security, str(err)))
			traceback.print_exc()

		logger.info('Success: %s Failure: %s' %  (success, failure))


def parse_arguments():
	'''parse arguments'''

	parser = argparse.ArgumentParser(description="Company daya")
	parser.add_argument('--root_dir', action='store', dest='root_dir',
										required=True, help='Root Directory')
	parser.add_argument('--config', action='store', dest='config',
										required=True, help='Config File name')
	result = parser.parse_args()
	return result



def extract_company_graph_info(config, run_date, root_dir):
	''' extract company graph '''
	
	base_url = config.get('COMPANY_GRAPH', 'COMPANY_URL')

	# get security universe
	sec_universe = get_security_universe_all(config)

	# spider
	spider = CompanySpider(base_url, root_dir)
	spider.extract_company_data(sec_universe, root_dir)

class SetupTask(luigi.Task):
	date = luigi.Parameter()
	config = luigi.Parameter()
	root_dir = luigi.Parameter()

	def requires(self):
		return []

	def output(self):
		return []

	def run(self):
		logger.info('Scrape company graph setup')

		# config
		config_db = read_config(self.config)

		# Logger setup
		app_name = 'datafeed::company_graph'
		syslog = SysLogHandler(address=(config_db.get('PAPERTRAIL', 'LOG_SERVER'), int(config_db.get('PAPERTRAIL', 'LOG_PORT'))))

		hostname = socket.gethostname() if socket.gethostname() else socket.getfqdn()
		formatter = logging.Formatter('{0}:%(asctime)s.%(msecs)d %(levelname)s %(module)s - %(funcName)s: %(message)s'.format(hostname),'%Y-%m-%d %H:%M:%S')

		syslog.setFormatter(formatter)
		logger.addHandler(syslog)


class CleanupTask(luigi.Task):
	date = luigi.Parameter()
	config = luigi.Parameter()
	root_dir = luigi.Parameter()

	def requires(self):
		return []

	def output(self):
		return []

	def run(self):
		logger.info('Clean-up company graph info')

class ExtractTask(luigi.Task):
	date = luigi.Parameter()
	config = luigi.Parameter()
	root_dir = luigi.Parameter()

	def requires(self):
		return []

	def output(self):
		return []

	def run(self):

		# read config
		config_db = read_config(self.config)

		# extract company graph info
		extract_company_graph_info(config_db, str(self.date), self.root_dir)

class CompanyGraphTask(luigi.WrapperTask):

	date=luigi.DateParameter(default=datetime.datetime.today())
	config=luigi.Parameter(default='/home/ubuntu/datafeed.cfg')
	root_dir=luigi.Parameter(default='/var/feed/data/companies')

	def requires(self):

		# set up Task
		yield SetupTask(date=self.date,config=self.config,root_dir=self.root_dir)

        # extract Task
		yield ExtractTask(date=self.date,config=self.config,root_dir=self.root_dir)

		# clean up Task
		yield CleanupTask(date=self.date,config=self.config,root_dir=self.root_dir)


if __name__ == "__main__":
	'''
		luigi --module=company_graph CompanyGraphTask --config=/home/ubuntu/datafeed.cfg --root-dir=/var/feed/data/companies --local-scheduler
	'''

	luigi.run()
