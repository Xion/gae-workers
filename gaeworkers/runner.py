'''
Runner module for gae-workers.
This module should be accessible as a request handler for URL
which is specified in the config.WORKER_URL.

Created on 2011-08-31

@author: xion
'''
from . import config
from gaeworkers.worker import _TASK_HEADER_INVOCATION
from google.appengine.api import memcache
from google.appengine.ext import webapp
from google.appengine.ext.webapp.util import run_wsgi_app
from google.appengine.runtime import DeadlineExceededError
import inspect
import logging


_WORKER_MEMCACHE_KEY = "worker://%(id)s"

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
        
        self.execute_worker(worker_id, worker_class)
        
        
    def execute_worker(self, id, class_obj):
        '''
        Commences execution of given worker.
        @param id: ID of the worker; used to obtain worker state (if any)
        @param class_obj: Worker class
        '''
        worker_name = self.request.headers['X-AppEngine-TaskName']
        worker = class_obj(worker_name, id)
        
        mc_key = _WORKER_MEMCACHE_KEY % {'id':id}
        worker_data = memcache.get(mc_key, namespace = config.MEMCACHE_NAMESPACE) #@UndefinedVariable
        if not worker_data: first_run = True
        else:
            # TODO: restore worker data from memcache
            first_run = False
        
        if first_run:   worker.setup()

        try:
            if not inspect.isgeneratorfunction(worker.run):
                logging.warning("[gae-workers] Worker's run() is not a generator function")
                worker.run()
            else:
                # TODO: measure the time of each iteration to estimate
                # the remaining time for the request and save worker data
                # in memcache deadline looms
                worker_run = worker.run()
                try:
                    while True:
                        worker_run.next()
                except StopIteration:
                    pass
        except DeadlineExceededError:
            logging.warning('[gae-workers] Task deadline exceeded for worker %s', worker._name)
            
            # enqueue further execution
            queue_name = self.request.headers['X-AppEngine-QueueName']
            invocation = self.request.headers.get(_TASK_HEADER_INVOCATION, 1)
            task = worker._create_task(invocation + 1)
            task.add(queue_name)
            
        # TODO: save worker data to memcache


worker_app = webapp.WSGIApplication([
                                     (config.WORKER_URL, WorkerHandler),
                                    ])


if __name__ == '__main__':
    run_wsgi_app(worker_app)
