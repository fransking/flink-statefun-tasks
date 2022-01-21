Extensions
==========

These are optional behaviours that can be enabled but are disabled by default.


Inline Tasks
------------

Python functions decorated with @tasks.bind() must be deployed with the worker.  In most production setups this is the more suitable behaviour
but in test environments or when working with interactive notebooks it may be useful to declare a task on the fly on the client side. 

Clearly there are security considerations when accepting pickled code so ensure you trust your callers and only enable this functionality with good reason.

On the worker:

.. code-block:: python

    from statefun_tasks.extensions.inline_tasks import enable_inline_tasks
    enable_inline_tasks(tasks)
    

On the client:

.. code-block:: python

    from statefun_tasks.extensions.inline_tasks import enable_inline_tasks, inline_task
    enable_inline_tasks(tasks)


    @inline_task()
    def hello_world():
        return 'Hello World'


    pipeline = hello_world.send()
    result = await client.submit_async(task)


State offloading
----------------

When running a large number of parallel tasks via in_parallel(), the pipeline must store and then aggregate each result before passing to the next task or 
returning the result to the egress or caller.  Depending on what the tasks return this could cause the state to grow beyond the memory constraints of a single worker.
If that is the case, offloading this state to external storage may be a solution.  If enabled, once state size exceeds a configured threshold, further items are offloaded 
to a storage backend and the state then holds a pointer to this data.

The same can apply when a large number of parallel requests are deferred due to a max_parallelism setting or the pipeline being paused.

Generally the recommendation is to pass pointers to large data sets with offloading provided as a best effort convenience.  

There is an example S3 based backend in the extensions/s3_storage folder.


.. code-block:: python

    from statefun_tasks import StorageBackend

    class CustomStorageBackend(StorageBackend):
        ...
    

    tasks.set_storage_backend(storage)
