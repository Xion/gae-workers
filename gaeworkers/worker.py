'''
Module containing the Worker class.

Created on 2011-08-31

@author: Xion
'''
import logging
import os
import hashlib
import uuid


class InvalidWorkerState(Exception):
    ''' Exception signaling invalid worker state for requested operation. '''
    def __init__(self, msg):
        self.msg = msg
    def __str__(self):
        return self.msg
        

class Worker(object):
    '''
    Base class for worker objects.
    '''
    def __init__(self, name = None):
        ''' Constructor. In most cases it doesn't need to be overidden.
        If it is, super() shall be called. '''
        self._name = name
        
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
        worker_id = getattr(self, '_id', None)
        if worker_id:   raise InvalidWorkerState('Worker is already running')
        
        worker_id = _generate_worker_id()
        self._id = worker_id
        
        # ...
        

def _generate_worker_id():
    '''
    Internal method for generating unique IDs for the workers.
    Method is pretty simple: get random UUID, hash it with MD5
    and return a hex string.
    '''
    random_uuid = uuid.uuid4().bytes
    id = hashlib.md5(random_uuid)
    return id
    
