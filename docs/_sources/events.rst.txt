Events
======

Flink Tasks raises events at various points during the execution of a task. 


Worker Events
-------------


On Task Received
^^^^^^^^^^^^^^^^

Raised when a task is received but before it is recorded in state or begins to execute.

.. code-block:: python

    @tasks.events.on_task_received
    def on_task_received(context, task_request):
        pass


On Task Started
^^^^^^^^^^^^^^^

Raised when a task begins to execute.

.. code-block:: python

    @tasks.events.on_task_started
    def on_task_started(context, task_request):
        pass


On Task Retry
^^^^^^^^^^^^^

Raised if a task fails and is going to be retried due to a RetryPolicy.  The retry_count is the number of times the task has retried including this one.

.. code-block:: python

    @tasks.events.on_task_retry
    def on_task_retry(context, task_request: TaskRequest, retry_count):
        pass


On Task Finished
^^^^^^^^^^^^^^^^

Raised when a task finishes with either a task_result or a task_exception.  If the task has returned a pipeline is_pipeline will be True.

.. code-block:: python

    @tasks.events.on_task_finished
    def on_task_finished(context, task_result=None, task_exception=None, is_pipeline=False):
        pass


On Emit Result
^^^^^^^^^^^^^^

Raised when task or pipeline is finished and the result is about to be emitted but before it is recorded in state.

TasksExceptions raised by this event handler will be ignored.

.. code-block:: python

    @tasks.events.on_emit_result
    def on_emit_result(context, task_result=None, task_exception=None):
        pass


Pipeline Events
---------------

The `pipeline <https://github.com/fransking/flink-statefun-tasks-embedded>`_ function also emits events to egress as the pipeline progresses.

.. code-block:: protobuf

    message PipelineCreated {
        string caller_id = 1;
        string caller_address = 2;
        PipelineInfo pipeline = 3;
    }

    message PipelineStatusChanged {
        TaskStatus status = 1;
    }

    message PipelineTasksSkipped {
        repeated TaskInfo tasks = 1;
    }

    message Event {
        string pipeline_id = 1;
        string pipeline_address = 2;
        string root_pipeline_id = 3;
        string root_pipeline_address = 4;

        oneof event {
            PipelineCreated pipeline_created = 5;
            PipelineStatusChanged pipeline_status_changed = 6;
            PipelineTasksSkipped pipeline_tasks_skipped = 7;
        }
    }

