Effects
=======

Effects apply an action to the pipeline in addition to returning a result, for example pausing the pipeline if some condition is met.


.. code-block:: python

    from stateful_tasks.effects import with_pause_pipeline


    @tasks.bind()
    def multiply(x, y):
        result = x * y
        return with_pause_pipeline(result) if result > 5 else result

    
    pipeline = multiply.send(3, 2).continue_with(divide, 2)
    
    result = await client.submit_async(pipeline)  # pipeline will pause after the first task
