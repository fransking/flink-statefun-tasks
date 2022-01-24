Deployment Topologies
=====================

Each task has an associated namespace and worker name corresponding to the namespace and type of a Flink function registered in the Statefun module.yaml.  The simplist deployment
topology involves registering a single function (call it example/worker) and connecting a single ingress topic (call it task.requests) to that function.

This may not be the optimal topology where pipelines are concerned.  When a pipeline is sent to ingress by the client and picked up by a worker, or similarly another task 
returns its own pipeline, these tasks become orchestrators for all sub-tasks in the pipeline.  Logically we depict a pipeline as:

.. code-block:: python

    ingress -> a() -> b() -> c() -> egress 

but in reality there is a pipeline function p() acting as the orchestrator:

.. code-block:: python

    ingress -> p() -> a() -> p() -> b() -> p() -> c() -> p() -> egress


Running p() on resource constrained workers may impact performance and it may make sense to hive these off into their own Flink function type.  Our deployments expose only
the pipeline workers to ingress.  Other worker functions are then registered seperately in the module.yaml such as example/cpu_worker, example/gpu_worker, example/aio_worker for different
classifictions of work. 


.. code-block:: python

    @tasks.bind(worker_name='cpu_worker'):
    def a():
        # do CPU bound work
        pass


    @tasks.bind(worker_name='gpu_worker'):
    def b():
        # do work that requires GPU
        pass


    @tasks.bind(worker_name='aio_worker'):
    async def c():
        # do IO bound work
        pass


    @tasks.bind(worker_name='pipeline_worker')
    def example_workflow():
        return a.send().continue_with(b).continue_with(c)


    pipeline = example_workflow.send()
    result = await client.submit_async(pipeline)
