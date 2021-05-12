Introduction
============

What is it?
^^^^^^^^^^^

A lightweight API that borrows concepts from Celery to allow Python developers to run task based workflows on Apache Flink Stateful Functions.  
Workflows are composed of Tasks which accept parameters and can be chained together as continuations into a Pipeline.  
The Pipeline becomes the Flink state.


Motivation
^^^^^^^^^^

I use Celery in my professional life to orchestrate distributed Python workflows.  These workflows are typically 1-N-1 such as:

* Load a portfolio of stocks
* For each stock load a timeseries of historical prices
* For each timeseries calculate the standard deviation
* Return the average of of the standard deviations

This is fairly trivial to achieve in Celery using plain old functions decorated as Celery tasks

.. code-block:: python

    @app.task
    def load_timeseries(stock: str):
        # load timeseries from market data system and return array of prices
        return prices


    @app.task
    def compute_std_dev(prices: list):
        return np.std(prices)


    @app.task
    def compute_average(std_devs: list)
        return np.mean(std_devs)

which could be run on Celery as a workflow

.. code-block:: python

    stocks = ['BT.L', 'DAIGn.DE', 'BP.L']

    workflow = 
        chain(group([chain(
            load_timeseries.s(stock),
            compute_std_dev.s())
        for stock in stocks]),
            compute_average.s()
        )

    result = workflow.delay()

Since the workflow is implemented as simple functions it is also testable and debuggable without having to spin up Celery

.. code-block:: python

    test_result = compute_average([compute_std_dev(load_timeseries('BT.L')), compute_std_dev(load_timeseries('VOD.L'))])


Flink Stateful Functions
^^^^^^^^^^^^^^^^^^^^^^^^

Let's revisit our stocks example and try to implement it as Flink Stateful Functions.  

.. code-block:: python

    @functions.bind('examples/load_timeseries')
    def load_timeseries(context, message):
        prices = _load_prices(message.as_type(STOCK))
        context.send('prices -> examples/compute_std_dev')


    @functions.bind('examples/compute_std_dev')
    def compute_std_dev(context, message):
        std_dev = np.std(message.as_type(PRICES))
        context.send('std_dev -> examples/compute_average')


    @functions.bind('examples/compute_average')
    def compute_average(context, message):
        context.storage.std_devs.append(message.as_type(STD_DEV))

        if len(context.storage.std_devs) > NUMBER_OF_STOCKS:
            avg = np.mean(context.storage.std_devs)
            context.send_egress('avg -> egress topic')


Some issues with this:

1. load_timeseries() always calls compute_std_dev().  It's no longer a resusable function so I cannot use it in other workflows. The same is true for compute_std_dev().

2. compute_average() has to wait for all standand deviations to be received before it calculates the average, storing intermdiate values in state

3. None of the functions are fruitful so they cannot be tested by chaining them together outside of Flink


A better approach might be to have a central orchestration function that load_timeseries(), compute_std_dev(), compute_average() call back to.  This makes them resusable
and keeps the state management in one place


.. code-block:: python

    @functions.bind('examples/load_timeseries')
    def load_timeseries(context, message):
        context.send('prices -> context.caller')


    @functions.bind('examples/compute_std_dev')
    def compute_std_dev(context, message):
        context.send('std_dev -> context.caller')


    @functions.bind('examples/compute_average')
    def compute_average(context, message):
        context.send('avg -> context.caller')


    @functions.bind('examples/load_timeseries')
    def compute_average_std_devs_of_timeseries(context, message):

        if message.is_type(STOCK_LIST):
            context.storage.stocks = message.as_type(STOCK_LIST)
            context.storage.std_devs = []
            context.storage.initial_caller = context.caller

            for stock in context.storage.stocks:
                context.send('stock -> examples/load_timeseries')
        
        elif message.is_type(PRICES):
            context.send('prices -> examples/compute_std_dev')

        elif message.is_type(STD_DEV):
            context.storage.std_devs.append(message.as_type(STD_DEV))

            if len(context.storage.std_devs) == len(context.storage.stocks)
                context.send('context.storage.std_devs -> examples/compute_average')

        elif message.is_type(AVG):
            context.send('average -> context.storage.initial_caller')



Flink Tasks
^^^^^^^^^^^

Flink Tasks wraps up this orchestration function into a pipeline so that developers can focus on writing simple functions that are 
combined into workflows using an intuitive API based around ordinary Python functions.

.. code-block:: python

    tasks = FlinkTasks(
        default_namespace="example", 
        default_worker_name="worker", 
        egress_type_name="example/kafka-generic-egress")


    @tasks.bind()
    def compute_average_std_devs_of_timeseries(stocks):
        return in_parallel(
            [load_timeseries.send(stock).continue_with(compute_std_dev) for stock in stocks]
        ).continue_with(compute_average)


    @tasks.bind()
    def load_timeseries(stock):
        return _load_prices(stock)


    @tasks.bind()
    def compute_std_dev(prices):
        return np.std(prices)


    @tasks.bind()
    def compute_average(std_devs):
        return np.mean(std_devs) 


    @functions.bind("example/worker")
    async def worker(context, message):
        try:
            await tasks.run_async(context, message)
        except Exception as e:
            print(f'Error - {e}')
            traceback.print_exc()
