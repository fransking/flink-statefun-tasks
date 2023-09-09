Deployment Topologies
=====================

A basic deployment topology involves an `embedded pipeline <https://github.com/fransking/flink-statefun-tasks-embedded>`_ function connected to an ingress topic plus a number of worker functions.


This code

.. code-block:: python

    from statefun_tasks import FlinkTasks()

    
    tasks = FlinkTasks(
        default_namespace="example",                        # default namespace for worker tasks
        default_worker_name="generic_worker",               # default type for worker tasks
        egress_type_name="example/kafka-generic-egress",    # egress to use for emitting results
        embedded_pipeline_namespace="example",              # namespace of the embedded pipeline function
        embedded_pipeline_type="embedded_pipeline")         # type of the embedded pipeline function


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


    @tasks.bind(worker_name='generic_worker')
    def example_workflow():
        return a.send().continue_with(b).continue_with(c)


    pipeline = example_workflow.send()
    result = await client.submit_async(pipeline)


corresponds to the following setup in the Flink module.yaml

.. code-block:: yaml

    kind: io.statefun_tasks.v1/pipeline
    spec:
      id: example/embedded_pipeline               # function namespace/type
      stateExpiration: PT1M                       # state expiration (ISO-8601)
      egress: example/kafka-generic-egress        # task response egress
      eventsEgress: example/kafka-generic-egress  # events egress (optional)
      eventsTopic: statefun-tasks.events          # events topic (optional)
      callbackDelayMs: 10                         # callback delay (optional)
    ---
    kind: io.statefun.endpoints.v2/http
    spec:
      functions: example/cpu_worker
      urlPathTemplate: http://cpu_worker:8085/statefun
    ---
    kind: io.statefun.endpoints.v2/http
    spec:
      functions: example/gpu_worker
      urlPathTemplate: http://gpu_worker:8085/statefun
    ---
    kind: io.statefun.endpoints.v2/http
    spec:
      functions: example/aio_worker
      urlPathTemplate: http://aio_worker:8085/statefun
    ---
    kind: io.statefun.endpoints.v2/http
    spec:
      functions: example/generic_worker
      urlPathTemplate: http://generic_worker:8085/statefun
    ---
    kind: io.statefun.kafka.v1/ingress
    spec:
      id: example/worker
      address: kafka-broker:9092
      consumerGroupId: flink-cluster-id
      startupPosition:
        type: earliest
      topics:
        - topic: statefun-tasks.requests
          valueType: io.statefun_tasks.types/statefun_tasks.TaskRequest
          targets:
            - example/embedded_pipeline
        - topic:  statefun-tasks.actions
          valueType: io.statefun_tasks.types/statefun_tasks.TaskActionRequest
          targets:
            - example/embedded_pipeline
    ---
    kind: io.statefun.kafka.v1/egress
    spec:
      id: example/kafka-generic-egress
      address: kafka-broker:9092
      deliverySemantic:
        type: exactly-once
        transactionTimeout: 15min
