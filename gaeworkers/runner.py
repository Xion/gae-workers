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


class WorkerHandler(webapp.RequestHandler):
    '''
    Request handler for workers. It is invoked by the GAE taskqueue
    and controls the execution of a single worker, preserving and
    restoring its state as necessary.
    '''
    def post(self):
        '''
        POST handler, invoked by GAE as a task.
        '''
        pass


worker_app = webapp.WSGIApplication([
                                     (config.WORKER_URL, WorkerHandler),
                                    ])


if __name__ == '__main__':
    run_wsgi_app(worker_app)
