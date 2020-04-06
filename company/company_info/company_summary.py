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

from summary_scrapper import scrape_articles

from utils import read_csv_file

from utils.secmaster import get_security_universe_with_exch

from utils import read_config
from utils.context_filter import ContextFilter

from logging.handlers import SysLogHandler

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addFilter(ContextFilter())


def parse_arguments():
	'''parse arguments'''

	parser = argparse.ArgumentParser(description="Company daya")
	parser.add_argument('--root_dir', action='store', dest='root_dir',
										required=True, help='Root Directory')
	parser.add_argument('--config', action='store', dest='config',
										required=True, help='Config File name')
	result = parser.parse_args()
	return result



def extract_company_subgraph(config, run_date, root_dir):
	''' extract company graph '''

	# company url	
	base_url = config.get('COMPANY_GRAPH', 'SUBGRAPH')

	# driver path
	firefox_driver = config.get('FIREFOX', 'DRIVER_PATH')
	
	# get security universe
	sec_universe = get_security_universe_with_exch(config)

	# scrape articles
	scrape_articles(run_date, base_url, root_dir, firefox_driver, sec_universe)
	logger.info('Extracting subgraph completed')

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
		extract_company_subgraph(config_db, str(self.date), self.root_dir)

class CompanySummaryTask(luigi.WrapperTask):

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
		luigi --module=company_summary CompanySummaryTask --config=/home/ubuntu/datafeed.cfg --root-dir=/var/feed/data/product --local-scheduler
	'''

	luigi.run()
