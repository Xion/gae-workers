'''
Module containing the Worker class.

Created on 2011-08-31

@author: Xion
'''
from . import config
from google.appengine.api.taskqueue import Task
import hashlib
import logging
import urllib
import uuid


class InvalidWorkerState(Exception):
    ''' Exception signaling invalid worker state for requested operation. '''
    def __init__(self, msg):
        self.msg = msg
    def __str__(self):
        return self.msg
        
        
_TASK_HEADERS_PREFIX = 'X-GAEWorkers-'
_TASK_HEADER_INVOCATION = _TASK_HEADERS_PREFIX + 'Invocation'

class Worker(object):
    '''
    Base class for worker objects.
    '''
    queue_name = config.QUEUE_NAME
    
    def __init__(self, name = None, id = None):
        '''
        Constructor. In most cases it doesn't need to be overidden.
        If it is, super() shall be called.
        '''
        self._name = name
        self._id = id
        
    def _create_task(self, invocation = 1):
        '''
        Creates a Task object for this worker.
        This method is used internally by the gae-workers library.
        '''
        # construct URL for the worker task
        qs_args = {}
        qs_args['class'] = "%s.%s" % (self.__class__.__module__, self.__class__.__name__)
        qs_args['id'] = self._id
        task_qs = urllib.urlencode(qs_args)
        task_url = "%s?%s" % (config.WORKER_URL, task_qs)
        
        headers = {
                   _TASK_HEADER_INVOCATION: invocation,
                   }
        return Task(name = self._name or self._id,
                    url = task_url, method = 'GET', headers = headers)
    
        
    def setup(self):
        '''
        Performs one-time initialization of the worker.
        This method is invoked by gae-workers task handler 
        at the beginning of worker's execution.
        '''
        logging.info('[gae-workers] %s initialized' % self.__class__.__name__)
        
    def run(self):
        '''
        Method that should be overridden in derived classes to do
        some actual work.
        '''
        logging.warning("[gae-workers] Empty Worker.run() method invoked")


    def start(self):
        '''
        Starts the worker by queuing a task that will commence its execution.
        '''
        if getattr(self, '_id', None):
            raise InvalidWorkerState('Worker is already running')
        self._id = _generate_worker_id()
        
        task = self._create_task()
        task.add(self.queue_name or config.QUEUE_NAME)
        
        

def _generate_worker_id():
    '''
    Internal method for generating unique IDs for the workers.
    Method is pretty simple: get random UUID, hash it with MD5
    and return a hex string.
    '''
    random_uuid = uuid.uuid4().bytes
    id = hashlib.md5(random_uuid)
    return id
    
