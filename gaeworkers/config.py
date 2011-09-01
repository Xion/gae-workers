'''
Configuration module.

Created on 2011-08-31

@author: xion
'''

# URL for the workers' runner. The runner.py script should be
# accessible under this URL for the library to function correctly.
WORKER_URL = '/_ah/worker'

# Minimum number of seconds gae-workers will reserve for storing the worker's
# state in memcache and delegating work to next task.
# Depending on actual estimates from running worker's code, the actual time
# before dropping work in current task may be higher. 
SAFETY_MARGIN = 5


###############################################################################


# Name of the taskqueue where worker tasks are being put in.
# You can use it to customize the processing rate and/or other params in queue.yaml.
# You don't generally need to change it.
QUEUE_NAME = '__gae-workers'

# Namespace which will be used by gae-workers to persist information
# about workers and their data.
# You don't generally need to change this.
MEMCACHE_NAMESPACE = 'gae-workers'

# A total amount of seconds request is allowed to be ran on App Engine.
# Currently, it is 10 minutes.
# You shouldn't need to modify this unless the deadline limit is changed in GAE. 
DEADLINE_SECONDS = 10 * 60
