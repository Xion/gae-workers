'''
Module containing the Worker class.

Created on 2011-08-31

@author: Xion
'''
from gaeworkers import config, data
from google.appengine.api import memcache
from google.appengine.api.taskqueue import Task
import hashlib
import logging
import urllib
import uuid


class InvalidWorkerState(Exception):
    ''' Exception signaling invalid worker state for requested operation. '''
    def __init__(self, msg):
        super(InvalidWorkerState, self).__init__(msg)
        self.msg = msg
    def __str__(self):
        return self.msg
        
        
_TASK_HEADERS_PREFIX = 'X-GAEWorkers-'
_TASK_HEADER_INVOCATION = _TASK_HEADERS_PREFIX + 'Invocation'

_WORKER_MESSAGES_MEMCACHE_KEY = "worker://%(id)s/messages"

class Worker(object):
    '''
    Base class for worker objects.
    '''
    queue_name = config.QUEUE_NAME
    
    # API "calls" available to workers
    SLEEP = staticmethod(lambda secs: ("sleep", (secs,)))
    FORK = staticmethod(lambda: ("fork", ()))
    GET_MESSAGES = staticmethod(lambda: ("get_messages", ()))
    
    def __init__(self, worker_name = None, worker_id = None):
        '''
        Constructor. In most cases it doesn't need to be overidden.
        If it is, super() shall be called.
        '''
        self._name = worker_name
        self._id = worker_id
        
    def _create_task(self, invocation = 1, eta = None):
        '''
        Creates a Task object for this worker.
        This method is used internally by the gae-workers library.
        @param invocation: Invocation count for this worker, passed as header
        @param eta: ETA (earliest execution time) for the task
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
                    url = task_url, method = 'GET', headers = headers,
                    eta = eta)
        
    def _get_state_dict(self):
        '''
        Retrieves the dictionary of worker attributes that consist of its state.
        @return: Worker's state dictionary
        '''
        return dict((attr, value)
                    for attr, value in self.__dict__.iteritems()
                    if not attr.startswith('_'))
    
        
    def setup(self):
        '''
        Performs one-time initialization of the worker.
        This method is invoked by gae-workers task handler 
        at the beginning of worker's execution.
        '''
        logging.info('[gae-workers] %s initialized', self.__class__.__name__)
        
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
        
    
    def post_message(self, msg):
        '''
        Posts a message to worker of given ID. Worker will receive it
        the when calling Worker.GET_MESSAGES routine.
        @param msg: Message to pass to worker. This can be any object,
                    although simple Python types are recommended.
        @return: Whether the message was posted (this doesn't mean it was processed!)
        '''
        worker_id = getattr(self, '_id', None)
        if not worker_id:
            raise InvalidWorkerState('Worker object has not been initialized')
        
        mc_key = _WORKER_MESSAGES_MEMCACHE_KEY % {'id':worker_id}
        msg_queue = memcache.get(mc_key, namespace = config.MEMCACHE_NAMESPACE) #@UndefinedVariable
        msg_queue = msg_queue or []
        
        msg_queue.append(data.save_value(msg))
        if not memcache.set(mc_key, messages, namespace = config.MEMCACHE_NAMESPACE): #@UndefinedVariable
            logging.error("[gae-workers] Could not post message -- possible overflow of message queue")
            return False
        
        return True
                

def _generate_worker_id():
    '''
    Internal method for generating unique IDs for the workers.
    Method is pretty simple: get random UUID, hash it with MD5
    and return a hex string.
    '''
    random_uuid = uuid.uuid4().bytes
    worker_id = hashlib.md5(random_uuid).hexdigest()
    return worker_id
    
