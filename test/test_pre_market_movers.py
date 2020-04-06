import os
import rethinkdb as r
import shutil
import sys
import unittest

from bs4 import BeautifulSoup
from mock import patch
from mockthink import MockThink
from scrapy.http import Response, Request, TextResponse

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../premarket')))

from pre_market_movers import fetch_pre_hours_movers
from pre_market_movers import parse_pre_hours_movers
from pre_market_movers import parse_rows

HTML = '''<!doctype html>
<html>
<body id="body">
        <div class="genTable">
            <table>
                
                        <thead>
	                        <tr>
		                        <th width="175"><a>Name</a></th>
		                        <th><a>Symbol</a></th>
		                        <th><a>03/11/2019<br />Last Sale</a></th>
		                        <th><a>Pre-Market<br />Last Sale</a></th>
		                        <th><a>Pre-Market %Change</a></th>
		                        <th><a>Pre-Market Time</a></th>
	                        </tr>
                        </thead>
                    
		                <tr>
			                <td>ABC corps, Inc</td>
			                <td>
			                    <span class="TalignL">
						            <h3>
							            <a>
							                ABCI
							            </a>
						            </h3>
			                    </span>
				            </td>
				            <td>42.92</td>
				            <td></td>

				            <td><span class="green">0.40%<span class"green">&nbsp;&#9650;</span></span></td>
				            <td></td>
			            </tr>
			            <tr>
			                <td>ABCD corps, Inc</td>
			                <td>
			                    <span class="TalignL">
						            <h3>
							            <a>
							                ABCDI
							            </a>
						            </h3>
			                    </span>
				            </td>
				            <td>42.92</td>
				            <td></td>

				            <td><span class="green">0<span class"green"></span></td>
				            <td></td>
			            </tr>
			</table>
		</div>
</body>	
</html>'''


class PreMarketMoversTest(unittest.TestCase):

	@classmethod
	def setUpClass(self):
		self.soup = BeautifulSoup(HTML, 'lxml')
		self.div_table = self.soup.find('div', attrs={'class' : 'genTable'})

	def test_parse_pre_hours_movers(self):
		''' To test parse_pre_hours_movers '''

		results = parse_pre_hours_movers(self.div_table)

		output = results[0]
		self.assertEquals(output['symbol'], 'ABCI')
		self.assertEquals(output['last_sale'], 42.92)
		self.assertEquals(output['last_sale_unit'], 'Dollars')
		self.assertEquals(output['move'], 'up')
		self.assertEquals(output['change_percent'], 0.4)
		output = results[1]
		self.assertEquals(output['symbol'], 'ABCDI')
		self.assertEquals(output['last_sale'], 42.92)
		self.assertEquals(output['last_sale_unit'], 'Dollars')
		self.assertEquals(output['move'], '')
		self.assertEquals(output['change_percent'], 0.0)
		
	@patch('pre_market_movers.time.sleep')
	@patch('pre_market_movers.urlopen')
	@patch('pre_market_movers.BeautifulSoup')
	def test_fetch_pre_hours_movers(self, soup, http, sleep):
		''' To test fetch_pre_hours_movers '''
	
		soup.return_value = soup
		soup2 = BeautifulSoup('', 'lxml')
		soup.find.side_effect = [None, soup2, self.div_table]
		results = fetch_pre_hours_movers('pre_hour_url')

		output = results[0]
		self.assertEquals(output['symbol'], 'ABCI')
		self.assertEquals(output['last_sale'], 42.92)
		self.assertEquals(output['last_sale_unit'], 'Dollars')
		self.assertEquals(output['move'], 'up')
		self.assertEquals(output['change_percent'], 0.4)
		
	def test_parse_rows(self):
		''' To test parse_rows '''

		rows = self.div_table.find_all('tr')
		output = parse_rows(rows)
		self.assertEquals(output.get(1).get('Name'), 'ABC corps, Inc')
		self.assertEquals(output.get(2).get('Last Sale'), '42.92')

