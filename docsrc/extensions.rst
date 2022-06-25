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
