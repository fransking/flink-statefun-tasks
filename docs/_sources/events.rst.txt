Events
======

Flink Tasks raises events at various points during the execution of a pipeline.  These events can be used to track task run time, pipeline length, state size etc.
Execution flow can in some circumstances be interrupted by raising a TasksException in the event handler.  For example in on_task_finished an exception can be raised if the size of 
the result is greater than some limit in which case the caller will recieve that error instead.  


On Task Received
----------------

Raised when a task is received but before it is recorded in state or begins to execute.

.. code-block:: python

    @tasks.events.on_task_received
    def on_task_received(context, task_request):
        pass


On Task Started
---------------

Raised when a task begins to execute.

.. code-block:: python

    @tasks.events.on_task_started
    def on_task_started(context, task_request):
        pass


On Task Retry
-------------

Raised if a task fails and is going to be retried due to a RetryPolicy.  The retry_count is the number of times the task has retried including this one.

.. code-block:: python

    @tasks.events.on_task_retry
    def on_task_retry(context, task_request: TaskRequest, retry_count):
        pass


On Task Finished
----------------

Raised when a task finishes with either a task_result or a task_exception.  If the task has returned a pipeline is_pipeline will be True.

.. code-block:: python

    @tasks.events.on_task_finished
    def on_task_finished(context, task_result=None, task_exception=None, is_pipeline=False):
        pass


On Pipeline Created
-------------------

Raised when a task has returned a pipeline and it starts to run.


.. code-block:: python

    @tasks.events.on_pipeline_created
    def on_pipeline_created(context, pipeline: Pipeline):
        pass


On Pipeline Status Changed
--------------------------

Raised when the state of a pipeline changes e.g. from running to paused or completed.  Status can be read from the context.pipeline_state (PipelineState proto).

.. code-block:: python

    @tasks.events.on_pipeline_status_changed
    def on_pipeline_status_changed(context, pipeline: Pipeline):
        pass


On Pipeline Task Finished
-------------------------

Raised when the result of a task reaches the orchestrating pipeline task.  This can be useful in scenarios where the pipelines are run on a different venue/worker type (e.g. K8s) to the 
tasks that they invoke (e.g. a general purpose compute grid).  

.. code-block:: python

    @tasks.events.on_pipeline_task_finished
    def on_pipeline_task_finished(context, task_result=None, task_exception=None):
        pass


On Pipeline Tasks Skipped
-------------------------

Raised when tasks are skipped over by the pipeline because they are part of an orphaned branch when exceptionally tasks are used.  For example
a.send().continue_with(b).exceptionally(c) where 'a' raises an exception causing the pipeline to jump to 'c' skipping 'b'. 

TasksExceptions raised by this event handler will be ignored.

.. code-block:: python

    @tasks.events.on_pipeline_tasks_skipped
    def on_pipeline_tasks_skipped(context, skipped_tasks):
        pass


On Pipeline Finished
--------------------

Raised when a pipeline finishes and either produces a task_result or task_exception.

.. code-block:: python

    @tasks.events.on_pipeline_finished
    def on_pipeline_finished(context, pipeline: Pipeline, task_result=None, task_exception=None):
        pass


On Emit Result
--------------

Raised when task or pipeline is finished and the result is about to be emitted but before it is recorded in state.

TasksExceptions raised by this event handler will be ignored.

.. code-block:: python

    @tasks.events.on_emit_result
    def on_emit_result(context, task_result=None, task_exception=None):
        pass
