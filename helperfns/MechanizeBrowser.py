# -*- coding: utf-8 -*-

import mechanize
import http.cookiejar

class MechanizeBrowser(object):
        '''Mechanize Browser class that handles the mechanize features'''
        
        def __init__(self):
                self.__setup_browser()

        def __setup_browser(self):
                '''Setup the mechanize browser with all the required features'''
                # setting up browser for login
                self.browser = mechanize.Browser()
                self.cookie_jar = http.cookiejar.LWPCookieJar()
                self.browser.set_cookiejar(self.cookie_jar)

                # Browser options
                self.browser.set_handle_equiv(True)
                # self.browser.set_handle_gzip(True)
                self.browser.set_handle_redirect(True)
                self.browser.set_handle_referer(True)
                self.browser.set_handle_robots(False)
                self.browser.set_handle_refresh(
                        mechanize._http.HTTPRefreshProcessor(), max_time=1)
                self.browser.addheaders = [('User-agent', 'Chrome')]
                self.login_required = False

        def reset(self):
                '''Reset feature for cases where the website has to reset to base url'''
                
                raise NotImplementedError("Base class should override " + self.__class__.__name__ + '.reset')

        def open_url(self, url):
                '''Visits the url and returns the page source'''

                response = self.browser.open(url).read()
                return response

        def get_response(self):
                '''Return the page source of the current visited url'''
                
                return self.browser.response().read()

        def set_login_required(self):
                '''Set the login flag for website that require authentication'''
                
                self.login_required = True

        def login(self):
                '''Login implementation left for derived class'''
                
                if self.login:
                        raise NotImplementedError("Base class should override " + self.__class__.__name__ + '.login')

