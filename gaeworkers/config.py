'''
Configuration module.

Created on 2011-08-31

@author: xion
'''

# URL for the workers' runner. The runner.py script should be
# accessible under this URL for the library to function correctly.
WORKER_URL = '/_ah/worker'


# Name of the taskqueue where worker tasks are being put in.
# You can use it to customize the processing rate and/or other params in queue.yaml.
# You don't generally need to change it.
QUEUE_NAME = '__gae-workers'

# Namespace which will be used by gae-workers to persist information
# about workers and their data.
# You don't generally need to change this.
MEMCACHE_NAMESPACE = 'gae-workers'
