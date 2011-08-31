'''
Configuration module.

Created on 2011-08-31

@author: xion
'''

# URL for the workers' runner. The runner.py script should be
# accessible under this URL for the library to function correctly.
WORKER_URL = '/_ah/worker'


# Namespace which will be used by gae-workers to persist information
# about workers and their data.
# You don't generally need to change this.
MEMCACHE_NAMESPACE = '__gae-workers'
