Actions
=======

Overview
--------

Actions are out of band operations on existing pipelines enabling clients to query the status of a pipeline and recall the original request and result.


Pipeline Status
^^^^^^^^^^^^^^^

.. code-block:: python

    pipeline = multiply.send(3, 2).continue_with(divide, 2)
    client.submit(pipeline)                                     # non-blocking returns Future

    status = await client.get_status_async(pipeline)            # type: TaskStatus


Pipeline Request
^^^^^^^^^^^^^^^^

.. code-block:: python

    pipeline = multiply.send(3, 2).continue_with(divide, 2)
    client.submit(pipeline)                                     # non-blocking returns Future

    request = await client.get_request_async(pipeline)          # type: TaskRequest


Pipeline Result
^^^^^^^^^^^^^^^

.. code-block:: python

    pipeline = multiply.send(3, 2).continue_with(divide, 2)
    client.submit(pipeline)                                     # non-blocking returns Future

    request = await client.get_result_async(pipeline)           # type: TaskResult
