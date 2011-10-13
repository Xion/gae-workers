'''
Created on 2011-09-12

@author: xion
'''
from gaeworkers.worker import Worker


class ShellWorker(Worker):
    def setup(self):
        self.session = {}
        
    def run(self):
        while True:
            msgs = yield Worker.GET_MESSAGES()
            if not msgs:    continue
            
            results = []
            for msg in msgs:
                res = eval(msg, self.session)
                results.append(res)
                yield
                
            memcache.set('shell_results', results, namespace = 'gaeworkers_shell') #@UndefinedVariable
