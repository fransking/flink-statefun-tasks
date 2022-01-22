Stateful Tasks
==============

Since functions on Flink are inherently stateful, tasks may also be written statefully.  Each task entry in the pipeline
has a namespace, worker_name and task_id corresponding to the namespace, address and id of a Flink Stateful Function.

By default, when composing pipelines using the PipelineBuilder, each entry is given a unique task id and therefore its own isolated state.  By either 
entering a fixed task_id in the tasks.bind() decorator or using the .set() function when constructing a pipeline, task entries can be added to a pipeline
that share the same task_id and therefore state. 


.. code-block:: python

    @tasks.bind(with_context=True, task_id='memoised_multiply')
    def memoised_multiply(context, x, y):
        state = context.get_state() or {}

        key = f'{x},{y}'

        if not key in state:
            state[key] = x * y
        
        context.set_state(state)
        return state[key]


    pipeline = in_parallel([
        memoised_multiply.send(3, 2),                           # calculates and memoises result
        memoised_multiply.send(3, 2)                            # returns memoised result
        memoised_multiply.send(3, 2).set(task_id=str(uuid4()))  # has a different task id and therefore independent state
    }
    
    result = await client.submit_async(pipeline) 
