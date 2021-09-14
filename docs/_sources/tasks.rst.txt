Tasks
=====

Registering a Task
------------------

Any Python function can be attributed with *tasks.bind()* to register it as a Flink Task.  Multiple tasks may be invoked by a single
Stateful Function:

.. code-block:: python

    @tasks.bind()
    def multiply(x, y):
        return x * y


    @tasks.bind()
    def subtract(x, y):
        return x - y


    @functions.bind("example/worker", specs=tasks.value_specs())
    async def worker(context, message):
        await tasks.run_async(context, message)



Error Handling
--------------

Exceptions thrown by a task can either be passed back to the caller or they can trigger a retry.  Retry parameters are set using 
a RetryPolicy:

.. code-block:: python

    @tasks.bind(retry_policy=RetryPolicy(retry_for=[ValueError], max_retries=2, delay=timedelta(seconds=5), exponential_back_off=True))
    def unreliable_task():
        ...


Calling a Task
--------------

Tasks can be called using the FlinkTasksClient:

.. code-block:: python

    result = multiply(3, 2)             # direct function call for testing

    task = multiply.send(3, 2)  
    result = await client.submit_async(task)  # indirect invocation as a Flink Task
