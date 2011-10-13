'''
gae-workers demo application: shell

Created on 2011-09-12

@author: xion
'''
from demo.shell_worker import ShellWorker
from django.utils import simplejson as json
from google.appengine.api import memcache
import webapp2
import os
import time
import jinja2


jinja_env = jinja2.Environment(loader = jinja2.FileSystemLoader(os.path.dirname(__file__)))

class ShellRequestHandler(webapp2.RequestHandler):
    
    TEMPLATE_PATH = os.path.join(os.path.dirname(__file__), 'shell.jhtml')
    
    def get(self):
        worker = ShellWorker()
        worker.start()
        self.render(worker_id = worker._id)
        
    def post(self):
        """
        POST handler invoked by AJAX call.
        """
        input = self.request.get('input')
        if not input:
            self.respond_with("")
            return
        worker_id = self.request.get('id')
        if not worker_id:
            self.respond_with("")
            return
        
        shell_worker = ShellWorker(id = worker_id)
        shell_worker.post_message(input)
        
        result, has_result = self.__try_to_obtain_result()
        result = result if has_result else "<Failed to evaluate input>"
        
        resp = { 'input': input, 'result': result }   
        self.respond_with(json.dumps(resp))
        
    def __try_to_obtain_result(self):
        ''' Retrieves the result of shell command delegated to worker
        - or at least attempts to. '''
        # soft busy waiting... yes, I know
        result, has_result = None, False
        sleep_time = 0
        while sleep_time < 5:
            sleep_time += 1
            time.sleep(sleep_time)
            results = memcache.get('shell_results', namespace = 'gaeworkers_shell') #@UndefinedVariable
            if results:
                result = results[0]
                memcache.delete('shell_results', namespace = 'gaeworkers_shell') #@UndefinedVariable
                has_result = True
                break
            
        return result, has_result
        
        
    def render(self, **kwargs):
        params = { 'columns': 80, 'history_rows': 30, 'prompt_rows': 3 }
        params.update(kwargs)
        
        template = jinja_env.get_template(self.TEMPLATE_PATH)
        page_html = template.render(params)
        self.respond_with(page_html)
        
    def respond_with(self, response_text, status = None):
        if status is not None:  self.error(status)
        self.response.out.write(response_text)


app = webapp2.WSGIApplication([("/", ShellRequestHandler)])

