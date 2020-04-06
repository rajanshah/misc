# -*- coding: utf-8 -*-

import ast
import logging
import traceback

from utils import get_lexml

"""
Company √
Address √
Website √
Sector √
Industry √
Officers √
Competitors √
Ticker √
Product Info
Analyst Info
"""

logger = logging.getLogger('intellimind')

# Officers_link
def get_officers_link(source, security):
	try:
		lexml = get_lexml(source)
		return lexml.xpath('//*[@id="management"]/div[2]/div/a')[0].attrib['href']
	except Exception, err:
		logging.warn("No Management Link found for {0}".format(security))

def extract_officers_data(lexml):
	''' extract officers data '''

	table = lexml.xpath('//*[@id="companyNews"]/div/div[2]/table')[0]
	header = [text.strip() for text in table.xpath('./tbody/tr/th/text()')]
	data = [td.text_content().strip() for td in table.xpath('./tbody/tr/td')]
	officer_data = []
	try:
		for i in range(len(data)/len(header)):
			start = i*len(header)
			officer_data.append(dict(zip(header, data[i:i+len(header)])))
	except Exception, ex:
		logger.error('Error in extracting officers data %s' % str(ex.args))
	finally:
		return officer_data

def get_company_name(lexml):
	''' get company name '''

	company_name = None
	try:
		company_name = lexml.xpath('//*[@id="companyheader"]/div[1]/h3/text()')[0].strip()
	except Exception, ex:
		logger.error('Error in getting company name %s' % str(ex.args))
	finally:
		return company_name

def get_address(lexml):
	''' get address '''

	address = None
	try:
		address = ' '.join([text.strip().replace('\n-','') for text in lexml.xpath('//*[@id="gf-viewc"]/div/div/div[3]/div[1]/div/div[8]/text()') if text.strip()])
	except Exception, ex:
		logger.error('Exception in getting address data %s' % str(ex.args))
	finally:
		return address

def get_website(lexml):
	''' web site '''

	website = None
	try:
		website = lexml.xpath('//*[@id="gf-viewc"]/div/div/div[3]/div[1]/div/div[10]/div/a')[0].text_content().strip()
	except Exception, ex:
		logger.error('Exception in getting website data %s' % str(ex.args))
	finally:
		return website

def get_company_description(lexml):
	''' get company description '''

	description = None
	try:
		description = lexml.xpath('//*[@id="summary"]/div[2]/div/text()')[0].strip()
	except:
		logger.error('Exception in getting company description %s' % str(ex.args))
	finally:
		return description

def get_sector_industry(lexml):
	''' sector and industry data '''

	sector_info = None
	try:
		sector_info = tuple([text.strip() for text in lexml.xpath('//*[@id="related"]/div[4]/div/div[1]/a/text()')])
	except Exception, ex:
		logger.error('Exception in getting sector and industry %s' % str(ex.args))
	finally:
		return sector_info

def get_competitors(lexml):
	''' get competitors '''

	content = lexml.text_content()
	rows = content[content.find("rows:[{"):]
	rows = rows[rows.find("[{")+1:rows.find("}]")+1]
	rows = rows.replace("id","\"id\"")
	rows = rows.replace("values","\"values\"")
	competitors_data = []
	temp = []
	for row in rows.split("},"):
		if not "}" in row:
			row = row + "}"
		try:
			temp.append(ast.literal_eval(row))
		except Exception, ex:
			fields = row.split(",")
			fields[2] = "\"" + fields[2].replace("\"","") + "\""
			row = ','.join(fields)
			temp.append(ast.literal_eval(row))
			
			logger.error('Exception in getting security competitors %s' % str(ex.args))
			
	# ["AAPL","Apple Inc.","96.96","+0.51","chg","0.53","","537.59B","NASDAQ","22144","AAPL"]
	keys = ['ticker', 'company_name', 'price', 'change', "chg", "change_percent", "chart", "mkt_cap","fin","value","ticker"]
	competitors_data = [dict(zip(keys, data['values'])) for data in temp]
	return [competitor['ticker'] for competitor in competitors_data]

def get_exchange_security(lexml):
	''' exchange security '''
	
	exchange = None

	try:
		exchange = tuple(lexml.xpath('//*[@id="companyheader"]/div[1]/text()')[0].strip().split(",")[-1][:-1].strip().split(":"))
	except Exception, ex:
		logger.error('Exception in getting exchange data %s' % str(ex.args))
	finally:
		return exchange

def get_key_stats_ratios(lexml):
	''' key stats ratios '''

	stats_ratios = None
	try:
		stats_ratios = [text.strip() for text in lexml.xpath('//*[id="gf-viewc"]/div/div/div[3]/div[1]/div/div[6]/table/tr/td/text()')]
	except:
		logger.error('Excpetion in getting stats ratios %s' % str(ex.args))
	finally:
		return stats_ratios


def extract_company_info(security, source, company_source):
	"""Return the company information
	"""
	company = {
		"Market_Info": {},
		"Management_Info": {},
		"Address_Info": {},
		"Competitors_Info": {}
	}
	try:
		lexml = get_lexml(source)
		company_lexml = get_lexml(company_source)
		company['Market_Info'].update({
			"Ticker": get_exchange_security(lexml)[1],
			"Exchange": get_exchange_security(lexml)[0],
			"Company_Name": get_company_name(lexml),
			"Sector": get_sector_industry(lexml)[0],
			"Industry": get_sector_industry(lexml)[0]
		})
		company['Management_Info'] = extract_officers_data(company_lexml)
		company['Address_Info'].update({
			"Address": get_address(lexml),
			"Description": get_company_description(lexml)
		})
		company['Competitors_Info'] = get_competitors(lexml)
	except Exception, e:
		logging.warn("Error in extracting {0} information".format(security))
		traceback.print_exec()
	finally:
		return company
