import os
import rethinkdb as r
import shutil
import sys
import unittest

from bs4 import BeautifulSoup
from mock import patch
from mockthink import MockThink
from scrapy.http import Response, Request, TextResponse

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../afterhrs')))

from afterhrs_movers import fetch_after_hours_movers
from afterhrs_movers import parse_after_hours_movers
from afterhrs_movers import parse_rows

HTML = '''<!doctype html>
<html>
<body id="body">
        <div class="genTable">
            <table>
                
<thead>
	                            <tr><!-- table header -->
		                            <th><a>Name</a></th>
		                            <th><a>Symbol</a></th>
		                            <th><a>03/12/2019<br />Last Sale</a></th>
		                            <th><a>After Hours<br />Last Sale</a></th>
		                            <th><a>After Hours<br />%Change</a></th>
		                            <th><a>After Hours Time</a></th>
	                            </tr>
	                            <!-- begin data rows -->
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
			                    <td>42.63</td>
			                    <td>42.6300</td>
			                    <td><span class="">0</span></td>
			                    <td width="105">3/12/2019 6:09:38 PM</td>
		                    </tr>
                       
                            <tr>
                                <td>ABCD Inc.</td>
		                        <td>
		                            <span class="TalignL">
				                        <h3>
					                        <a>
					                            ABCDI
					                        </a>
				                        </h3>
		                            </span>
			                    </td>
			                    <td>263.51</td>
			                    <td>263.5000</td>
			                    <td><span class="red">0.00%</span></td>
			                    <td width="105">3/12/2019 6:07:54 PM</td>
		                    </tr>
			</table>
		</div>
</body>	
</html>'''

class AfterHoursMoversTest(unittest.TestCase):

	@classmethod
	def setUpClass(self):
		self.soup = BeautifulSoup(HTML, 'lxml')
		self.div_table = self.soup.find('div', attrs={'class' : 'genTable'})

	def test_parse_after_hours_movers(self):
		''' To test parse_after_hours_movers '''

		results = parse_after_hours_movers(self.div_table)

		output = results[0]
		self.assertEquals(output['symbol'], 'ABCI')
		self.assertEquals(output['last_sale'], 42.63)
		self.assertEquals(output['last_sale_unit'], 'Dollars')
		self.assertEquals(output['move'], '')
		self.assertEquals(output['after_hour_last_sale'], 42.63)
		self.assertEquals(output['change_percent'], 0.0)
		output = results[1]
		self.assertEquals(output['symbol'], 'ABCDI')
		self.assertEquals(output['last_sale'], 263.51)
		self.assertEquals(output['last_sale_unit'], 'Dollars')
		self.assertEquals(output['after_hour_last_sale'], 263.50)
		self.assertEquals(output['move'], 'down')
		self.assertEquals(output['change_percent'], 0.0)
		
	@patch('afterhrs_movers.time.sleep')
	@patch('afterhrs_movers.urlopen')
	@patch('afterhrs_movers.BeautifulSoup')
	def test_fetch_after_hours_movers(self, soup, http, sleep):
		''' To test fetch_after_hours_movers '''
		
		soup.return_value = soup
		soup2 = BeautifulSoup('', 'lxml')
		soup.find.side_effect = [None, soup2, self.div_table]
		results = fetch_after_hours_movers('after_hour_url')
		
		output = results[0]
		self.assertEquals(output['symbol'], 'ABCI')
		self.assertEquals(output['last_sale'], 42.63)
		self.assertEquals(output['last_sale_unit'], 'Dollars')
		self.assertEquals(output['move'], '')
		self.assertEquals(output['after_hour_last_sale'], 42.63)
		self.assertEquals(output['change_percent'], 0.0)
		
	def test_parse_rows(self):
		''' To test parse_rows '''
		
		rows = self.div_table.find_all('tr')
		output = parse_rows(rows)
		self.assertEquals(output.get(1).get('Name'), 'ABC corps, Inc')
		self.assertEquals(output.get(2).get('class_move'), 'down')
		self.assertEquals(output.get(2).get('Last Sale'), '263.51')

