Pipelines
=========

Continuations
-------------

Tasks can be combined into a pipeline using continuations:

.. code-block:: python

    # task results are passed as arguments to continuations
    pipeline = multiply.send(3, 2).continue_with(divide, 2)
    
    result = await client.submit_async(pipeline) 


Fan out / Fan in
----------------

The *in_parallel* function is used to submit tasks in parallel:

.. code-block:: python

    pipeline = in_parallel([
        multiply.send(3, 2),
        multiply.send(4, 5)
    ])
    
    result = await client.submit_async(pipeline)

Parallel tasks results can be aggregated into a continuation:

.. code-block:: python

    @tasks.bind()
    def average(values: list):
        return sum(values) / len(values) 


    pipeline = in_parallel([
        multiply.send(3, 2),
        multiply.send(4, 5)
    ]).continue_with(average)

Nesting is also permitted:

.. code-block:: python

    pipeline = in_parallel([
        multiply.send(3, 2).continue_with(divide, 2),  # continuation within in_parallel
        in_parallel([                                  # nested in_parallel
            multiply.send(4, 5),
            multiply.send(3, 4)
        ]).continue_with(average)
    ]).continue_with(average)


Parallel pipelines can have their concurrency limited using a semphore like max_parallelism parameter:

.. code-block:: python

    pipeline = in_parallel([
        multiply.send(3, 2),
        multiply.send(4, 5),
        ...
    ], max_parallelism=10)
    
    result = await client.submit_async(pipeline)

Passing State
-------------

State can be shared and passed across tasks in a pipeline.  Tasks that access state should
declare so in *@tasks.bind()*:

.. code-block:: python

    @tasks.bind(with_state=True)       # sets initial state
    def multiply(state, x, y):
        state = 10
        return state, x * y


    @tasks.bind()                      # state is passed across
    def subtract(x, y):
        return x - y


    @tasks.bind(with_state=True)       # accesses state
    def add_state(state, x):
        return state, state + x


    pipeline = multiply.send(3, 2) \   # 6
        .continue_with(subtract, 1) \  # 5
        .continue_with(add_state)      # 15


Accessing the Context
---------------------

A wrapper around the Flink context can also be accessed by declaring so in *@tasks.bind()*:

.. code-block:: python

    @tasks.bind(with_context=True)
    def task_using_context(context):
        caller = context.get_caller_id()
        return f'{caller}, you called me'



Error Handling
--------------

Any task within a pipeline may throw exceptions and if not caught by a retry these will terminate the pipeline.  
Unhandled exceptions are returned to the client as they are with single tasks.

Pipelines may also include a *finally_do* contiunation as their final step which will be called regardless of 
success or failure.  This is a good place to put any clean up logic.  

The *finally_do* task is non-fruitful so the result of the pipeline is the result of the previous task (or exception):

.. code-block:: python

    @tasks.bind(with_state=True)
    def cleanup(state, *args):
        # do cleanup


    pipeline = multiply.send(3, 2).finally_do(cleanup)


Orchestrator Tasks
------------------

As well as pipelines constructed client side, tasks may also return their own pipelines.  Orchestrator tasks support the following 
patterns:


Composition
^^^^^^^^^^^

.. code-block:: python

    @tasks.bind()
    def multiply_and_subtract(mult_a, mult_b, sub_c):
        return multiply.send(mult_a, mult_b).continue_with(subtract, sub_c)


    pipeline = multiply_and_subtract.send(3, 2, 1).continue_with(...)


Conditional Execution
^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    @tasks.bind()
    def add_positive(x, y):
        return add.send(x, y).continue_with(make_positive)


    @tasks.bind()
    def make_positive(x):
        if x > 0:
            return x                        # either return value
        else:
            return multiply.send(x, -1)     # or another pipeline


    pipeline = add_positive.send(-3, 2)


Recursion
^^^^^^^^^

.. code-block:: python

    @tasks.bind()
    def count_to_100(x):
        return add.send(x, 1).continue_with(check_result)


    @tasks.bind()
    def check_result(x):
        if x == 100:
            return x
        else:
            return count_to_100.send(x)     # recursive


    pipeline = count_to_100.send(0)
