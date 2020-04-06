# -*- coding: utf-8 -*-

from lxml import html

class LxmlScraper(object):
	'''LXML Scraper class'''
	
	def get_lexml(self, source):
		'''Returns the lxml element of the source'''
		
		return html.fromstring(source)
