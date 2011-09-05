'''
Runner module for gae-workers.
This module should be accessible as a request handler for URL
which is specified in the config.WORKER_URL.

Created on 2011-08-31

@author: xion
'''
from . import config, data
from datetime import datetime, timedelta
from google.appengine.api import memcache
from google.appengine.ext import webapp
from google.appengine.ext.webapp.util import run_wsgi_app
from google.appengine.runtime import DeadlineExceededError
from time import time
from worker import _TASK_HEADER_INVOCATION
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
            module_name, class_name = worker_class_name.rsplit('.', 1)
            worker_module = __import__(module_name, globals(), locals(), fromlist = [class_name])
            worker_class = getattr(worker_module, class_name)
        except ValueError:
            logging.error('[gae-workers] Invalid worker class name (%s)', worker_class_name)
            return
        except ImportError:
            logging.error("[gae-workers] Failed to import worker class' module (%s)", module_name)
            return
        except AttributeError:
            logging.error('[gae-workers] Module %s does not contain worker class %s', module_name, class_name)
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
        
        self.restore_worker_state(worker)
        if not worker._first_run:
            logging.debug("[gae-workers] Initializing state of worker '%s'", worker._name)
            worker.setup()

        try:
            if not inspect.isgeneratorfunction(worker.run):
                logging.warning("[gae-workers] Worker's run() is not a generator function")
                worker.run()
                finished = True
            else:
                logging.debug("[gae-worker] Running worker '%s'", worker._name)
                finished = self.run_worker(worker)
        except DeadlineExceededError:
            logging.warning('[gae-workers] Task deadline exceeded for worker %s', worker._name)
            self.save_worker_state(worker)
            finished = False
            
        if not finished:
            self.schedule_worker_execution(worker)            
        
    def run_worker(self, worker):
        '''
        Spins worker run() method, allowing its code to execute while measuring
        the time it takes and estimating the remaining time until deadline.
        This is the main method of the workers' runner.
        @param worker: Worker object to run. Its run() method shall be a generator function.
        @return: Whether worker's execution shall be continued
        '''
        worker_run = worker.run()
        finished = False
        
        # initialize measurement/statistical variables
        estimated_time_left = config.DEADLINE_SECONDS
        total_running_time = 0
        spins_count = 0
        max_spin_time = max_running_average = 0
        
        current_time = time()
        while estimated_time_left > 0:
            try:
                # invoke the next iteration and see whether it ended with an "API" call
                api_call = worker_run.next()
                if api_call:
                    try:
                        api_name, api_params = api_call
                    except ValueError:
                        logging.warning("[gae-workers] Invalid API call coming from worker %s (ID=%s): %s",
                                        worker._name, worker._id, api_call)
                    else:
                        # perform appropriate action
                        if api_name == 'sleep':
                            secs = api_params[0]
                            if secs < config.MIN_SLEEP_SECONDS:
                                logging.warning("[gae-workers] Worker %s (ID=%s) tried to sleep for % seconds -- that's too short",
                                                worker._name, worker._id, secs)
                            else:
                                self.save_worker_state(worker)
                                self.schedule_worker_execution(worker, delay = timedelta(seconds = secs))
                                return True # act as if worker finished to prevent double-queuing
                
                spin_finish_time = time()
                spin_duration = spin_finish_time - current_time
                current_time = spin_finish_time # intentionally including our own control code in measurement
                total_running_time += spin_duration
                spins_count += 1
                
                # update statistics
                max_spin_time = max(max_spin_time, spin_duration)
                running_average = total_running_time / float(spins_count)
                max_running_average = max(max_running_average, running_average)
                
                # if we don't seem to manage to squeeze in another spin, we finish this task
                if estimated_time_left - (max_running_average + config.SAFETY_MARGIN) <= 0:
                    break
                
                estimated_time_left -= spin_duration
            except StopIteration:
                logging.info("[gae-workers] '%s' finished", worker._name)
                finished = True
                break
        
        if not finished:        
            self.save_worker_state(worker)
        return finished
    
    def schedule_worker_execution(self, worker, delay = None):
        '''
        Queues up a next task that is to carry on execution of specified worker.
        @param delay: Whether the task should be delayed (timedelta object or None) 
        '''
        queue_name = self.request.headers['X-AppEngine-QueueName']
        invocation = self.request.headers.get(_TASK_HEADER_INVOCATION, 1)
        eta = datetime.now() + delay if delay else None
        
        task = worker._create_task(invocation + 1, eta)
        task.add(queue_name)
        logging.debug("[gae-workers] Worker '%s' (ID=%s) enqueued for further execution",
                      worker._name, worker._id)
        
                
    def save_worker_state(self, worker):
        '''
        Saves the worker state in memcache in order to retrieve it later,
        in subsequent tasks dedicated to run this worker.
        @param worker: Worker object whose state is to be saved
        '''
        state = {}
        for attr, value in worker.__dict__.iteritems():
            if attr.startswith('_'):    continue
            try:
                state[attr] = data.save_value(value)
            except data.DataError, e:
                logging.error("[gae-workers] Error while saving %s: %s", attr, e)
            
        mc_key = _WORKER_MEMCACHE_KEY % {'id': worker._id}
        if not memcache.set(mc_key, state, namespace = config.MEMCACHE_NAMESPACE): #@UndefinedVariable
            logging.error("[gae-workers] Failed to save state for worker '%s' (ID=%s)",
                          worker._name, worker._id)
            
    def restore_worker_state(self, worker):
        '''
        Loads the worker state from memcache if it was saved previously.
        @param worker: Worker object whose state is to be restored 
        '''
        mc_key = _WORKER_MEMCACHE_KEY % {'id': worker._id}
        state = memcache.get(mc_key, namespace = config.MEMCACHE_NAMESPACE) #@UndefinedVariable
        if state is None:
            worker._first_run = True
            return
        
        for attr, value in state.iteritems():
            try:
                value = data.restore_value(value)
                setattr(worker, attr, value)
            except data.DataError, e:
                logging.error("[gae-workers] Error while restoring %s: %s", attr, e)
                
        worker._first_run = False
        
        
        
if __name__ == '__main__':
    worker_app = webapp.WSGIApplication([
                                         (config.WORKER_URL, WorkerHandler),
                                        ])
    run_wsgi_app(worker_app)
