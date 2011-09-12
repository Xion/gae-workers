'''
gae-workers demo application: shell

Created on 2011-09-12

@author: xion
'''
from google.appengine.ext import webapp
from google.appengine.ext.webapp import WSGIApplication
from google.appengine.ext.webapp.util import run_wsgi_app
from google.appengine.ext.webapp import template
import os


class ShellRequestHandler(webapp.RequestHandler):
    
    TEMPLATE_PATH = os.path.join(os.path.dirname(__file__), 'shell.djhtml')
    
    def get(self):
        self.render()
        
    def render(self):
        page_html = template.render(self.TEMPLATE_PATH, {})
        self.response.out.write(page_html)
        


app = WSGIApplication(["/", ShellRequestHandler])

if __name__ == '__main__':
    run_wsgi_app(app)
