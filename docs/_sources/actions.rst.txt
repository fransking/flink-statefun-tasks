Actions
=======


Actions are used to query pipeline state or pause / resume / cancel a pipeline that is in flight.


Querying
--------


Pipeline Status
^^^^^^^^^^^^^^^

.. code-block:: python

    pipeline = multiply.send(3, 2).continue_with(divide, 2)
    client.submit(pipeline)                                     # non-blocking 'fire and forget'

    status = await client.get_status_async(pipeline)            # type: TaskStatus


Pipeline Request
^^^^^^^^^^^^^^^^

.. code-block:: python

    pipeline = multiply.send(3, 2).continue_with(divide, 2)
    client.submit(pipeline)                                     # non-blocking 'fire and forget'

    request = await client.get_request_async(pipeline)          # type: TaskRequest


Pipeline Result
^^^^^^^^^^^^^^^

.. code-block:: python

    pipeline = multiply.send(3, 2).continue_with(divide, 2)
    client.submit(pipeline)                                     # non-blocking 'fire and forget'

    request = await client.get_result_async(pipeline)           # type: TaskResult



Flow Control
------------

Pipelines may be paused, unpaused and cancelled

Pausing & Resuming Pipelines
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    pipeline = a_long_running_task.send().continue_with(save)
    client.submit(pipeline)                                     # non-blocking 'fire and forget'

    await client.pause_pipeline_async(pipeline)                 # pipline will be likely paused before the save task runs                 
    status = await client.get_status_async(pipeline)            # TaskStatus.PAUSED

    await client.unpause_pipeline_async(pipeline)               # pipline will be unpaused and save task will be scheduled                 
    status = await client.get_status_async(pipeline)            # TaskStatus.RUNNING or TaskStatus.COMPLETED


Cancelling a Pipeline
^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    pipeline = a_long_running_task.send().continue_with(save)
    client.submit(pipeline)                                     # non-blocking 'fire and forget'

    await client.cancel_pipeline_async(pipeline)                # pipline will be likely cancelled before the save task runs                 
    status = await client.get_status_async(pipeline)            # TaskStatus.CANCELLED
