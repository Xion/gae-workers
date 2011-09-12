'''
gae-workers demo application: shell

Created on 2011-09-12

@author: xion
'''
from google.appengine.dist import use_library
use_library('django', '1.2')

from django.utils import simplejson as json
from google.appengine.ext import webapp
from google.appengine.ext.webapp import WSGIApplication
from google.appengine.ext.webapp.util import run_wsgi_app
from google.appengine.ext.webapp import template
import os


class ShellRequestHandler(webapp.RequestHandler):
    
    TEMPLATE_PATH = os.path.join(os.path.dirname(__file__), 'shell.djhtml')
    
    def get(self):
        self.render()
        
    def post(self):
        """
        POST handler invoked by AJAX call.
        """
        input = self.request.body
        if not input:
            self.respond_with("")
            return
        
        result = '...'
        resp = { 'input': input, 'result': result }   
        self.respond_with(json.dumps(resp))
        
    def render(self):
        args = { 'columns': 80, 'history_rows': 30, 'prompt_rows': 3 }
        page_html = template.render(self.TEMPLATE_PATH, args)
        self.respond_with(page_html)
        
    def respond_with(self, response_text, status = None):
        if status is not None:  self.error(status)
        self.response.out.write(response_text)


app = WSGIApplication([("/", ShellRequestHandler)])

if __name__ == '__main__':
    run_wsgi_app(app)
