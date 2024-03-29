gae-workers
=

__Warning__: This project is under heavy development to bring it into some usable state first.
If you want to use it, proceed with caution. The actual functionality provided by library may
hardly resemble assumptions stated in this documentation.
In other words: stuff is being made here. Help, if you can, but don't cry if something breaks.

*Note*: This library is to be used with Python 2.7 runtime on GAE.


Overview
-
gae-workers is a library that enables to execute long-running processes ("workers") on Google App Engine
without the use of backends. For that, it uses a combination of task queue (for actual code execution)
and memcache (for preserving the state of workers).


Installation
-
Once downloaded, include the library with your App Engine application. You should also add an entry
to *app.yaml* that will make workers' tasks go to the *runner.py* script intended to handle them:

    handlers:
    - url: /_ah/worker
      script: gaeworkers/runner.app

If you want, you can change the URL (among other parameters) by editing the *config.py* file.
(and, of course, changing in *app.yaml* as well).


Usage
-
*Note*: Examples below assume that 'gaeworkers' directory is in your app's root directory.

Using a worker is quite similar to working with the Python standard <code>threading.Thread</code> class.
In general, you define a class inheriting from <code>gaeworkers.Worker</code> and implement
its <code>run()</code> method to do your logic:

```python
from gaeworkers import Worker
# ...
class MyWorker(Worker):
    def setup():
        self.query = Model.all()
    def run():
        for model in self.query:
            do_something(model)
            yield
```
There are few things to bear in mind though:

  * For best results, the <code>run()</code> method shall be a generator function that uses <code>yield</code>
    frequently. This allows *gaeworkers* to control the execution of the worker, measure the time it takes
    and estimate whether a deadline is looming and work shall be delegated to next task.
    Without the <code>yield</code>ing, all code in <code>run()</code> has to be executed in one go; Python
    does not allow preempting.
  * State of worker object is preserved between queued tasks that are used for executing worker's code. Therefore any
    non-volatile data shall be stored in <code>self</code>'s attributes.
  * <code>run()</code> is invoked "from the beginning" for every task spawned to handle the worker. Hence it is
    not a good place to have any sort of initialization. For that, implement the <code>setup()</code> method - it is
    ran only once per worker.

Starting a worker is straightforward:

```python
worker = MyWorker(name='Model worker')
worker.start()
```
Assigning a <code>name</code> allows for easily distinguishing tasks belonging to different workers in App Engine
logs and/or Appstats. The name is included in the query string worker's task URL, and is used as a name for the task.
