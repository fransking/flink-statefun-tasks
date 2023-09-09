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

Large parallelisms may be limited by the performance of a single pipeline function aggregating the results.  
In this case it is possible to split up the parallelism into multiple inline pipelines 'map/reduce' style
using the num_stages parameter:

.. code-block:: python

    pipeline = in_parallel([
        multiply.send(3, 2),
        multiply.send(4, 5),
        ...
    ], num_stages=10)
    
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


Error Handling
--------------

Any task within a pipeline may throw exceptions and if not caught by a retry these will terminate the pipeline.  
Unhandled exceptions are returned to the client as they are with single tasks.

Exceptions can be caught using *exceptionally* tasks

.. code-block:: python

    @tasks.bind()
    def handle_error(ex):
        # handle error either by returning a result 
        # or raising a new exception


    pipeline = multiply.send(3, 2).exceptionally(handle_error)

It is possible to have more than one exceptionally task in a pipeline

.. code-block:: python

    pipeline = a.send().exceptionally(b).continue_with(c).exceptionally(d).finally_do(e)


Pipelines may also include a *finally_do* task as their final step which will be called regardless of 
success or failure.  This is a good place to put any clean up logic.  

The *finally_do* task is non-fruitful so the result of the pipeline is the result of the previous task (or exception):

.. code-block:: python

    @tasks.bind(with_state=True)
    def cleanup(state, *args):
        # do cleanup


    pipeline = multiply.send(3, 2).finally_do(cleanup)


Setting Initial Parameters
--------------------------

Consider a pipeline that multiplies in parallel the numbers 1 to 10000 by 2.

.. code-block:: python

    pipeline = in_parallel([
        multiply.send(2, 1),
        multiply.send(2, 2),
        ...
        multiply.send(2, 10000)
    ], num_stages=10)

When serialised to protobuf the first parameter to each function is repeated in each serialised task.
To reduce message size this parameter can be set on the pipeline using the 'with_initial' function on the PipelineBuilder:

.. code-block:: python

    pipeline = in_parallel([
        multiply.send(1),
        multiply.send(2),
        ...
        multiply.send(10000)
    ], num_stages=10).with_initial(args=2)


Initial kwargs and task state may also be set this way.


Orchestrator Tasks
------------------

Tasks may also return pipelines allowing for dynamic workflows with features such as:


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


Inline Pipelines
----------------

State is isolated by default between a parent pipeline and any child pipelines that it creates.  This
is done on the assumption that a pipeline that calls a task that itself creates a pipeline would rather 
treat that child pipeline as a so called black box implementation and hence the states should be kept independent.  

This behaviour can be overriden by declaring the child pipeline as 'inline'.