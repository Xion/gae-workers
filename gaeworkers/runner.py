'''
Runner module for gae-workers.
This module should be accessible as a request handler for URL
which is specified in the config.WORKER_URL.

Created on 2011-08-31

@author: xion
'''
from . import config
from google.appengine.ext import webapp
from google.appengine.ext.webapp.util import run_wsgi_app
import logging


class WorkerHandler(webapp.RequestHandler):
    '''
    Request handler for workers. It is invoked by the GAE taskqueue
    and controls the execution of a single worker, preserving and
    restoring its state as necessary.
    '''
    def get(self):
        '''
        GET handler, invoked by GAE as a task.
        @note: Fatal (irrecoverable) exceptions shall be caught and quenched
               to avoid the task being queued again.
        '''
        worker_class_name = self.request.GET.get('class')
        if not worker_class_name:
            logging.error('[gae-workers] No worker class name provided')
            return
        worker_id = self.request.GET.get('id')
        if not worker_id:
            logging.error('[gae-workers] No worker ID provided')
            return
        
        # attempt to import the worker class
        try:
            worker_module, class_name = worker_class_name.rsplit('.')
            worker_class = __import__(worker_module, globals(), locals(), fromlist = [class_name])
        except ValueError:
            logging.error('[gae-workers] Invalid worker class name (%s)', worker_class_name)
            return
        except ImportError:
            logging.error('[gae-workers] Failed to import worker class (%s)', worker_class_name)
            return
        logging.debug('[gae-workers] Worker class %s imported successfully', worker_class_name)
        
        self.execute_worker(worker_class)
        
        
    def execute_worker(self, worker_class):
        pass


worker_app = webapp.WSGIApplication([
                                     (config.WORKER_URL, WorkerHandler),
                                    ])


if __name__ == '__main__':
    run_wsgi_app(worker_app)
