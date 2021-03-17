# Flink Tasks
Tasks API for Stateful Functions on Flink

## What is it?

A lightweight API that borrows concepts from Celery to allow Python developers to run task based workflows on Apache Flink Stateful Functions.  Workflows are composed of Tasks which accept parameters and can be chained together as continuations into a Pipeline.  The Pipeline becomes the Flink state.

Additional documentation can be found [here](https://fransking.github.io/flink-statefun-tasks).

```
@tasks.bind()
def greeting_workflow(first_name, last_name):
    return say_hello.send(first_name, last_name).continue_with(say_goodbye)


@tasks.bind()
def say_hello(first_name, last_name):
    return f'Hello {first_name} {last_name}'


@tasks.bind()
def say_goodbye(greeting):
    return f'{greeting}.  So now I will say goodbye'
```

## Motivation

I use Celery in my professional life to orchestrate distributed Python workflows.  These workflows are typically 1-N-1 cardinality such as:

* Load a portfolio of stocks
* For each stock load a timeseries of historical prices
* For each timeseries calculate the standard deviation
* Return the average of of the standard deviations

This is fairly trivial to achieve in Celery using plain old functions decorated as Celery tasks

```
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
```

which could be run on Celery as a workflow

```
stocks = ['BT.L', 'DAIGn.DE', 'BP.L']

workflow = 
    chain(group([chain(
        load_timeseries.s(stock),
        compute_std_dev.s())
    for stock in stocks]),
        compute_average.s()
    )

result = workflow.delay()
```

Since the workflow is implemented as simple functions it is also testable and debuggable without having to spin up Celery

```
test_result = compute_average([compute_std_dev(load_timeseries('BT.L'))])
```

Flink is interesting to me because unlike Celery, it is polygot and remote functions can be written in languages other than Python.  Nevertheless the existing workflows I run on Celery would need to be moved.  What would that look like?


### Flink Stateful Functions

If you have read this far and haven't seen Flink Stateful Functions before it would be worth reading the documentation on the Apacke Flink site.  Flink doesn't really have defined workflows.  Functions can call other functions, functions can reply to functions that called them and functions can emit results to some egress, typically a Kakfa topic.  This makes it less constrained than Celery but at the same time puts more work on the developer.  

Let's revisit our stocks example from a Flink perspective.  

```
@functions.bind('examples/load_timeseries')
def load_timeseries(context, stock):
    prices = _load_prices(stock)
    context.send('examples/compute_std_dev', prices)


@functions.bind('examples/compute_std_dev')
def compute_std_dev(context, prices):
    context.reply(np.std(prices))
```

Some issues with this:

1. load_timeseries() always calls compute_std_dev().  It's no longer a resusable function so I cannot use it in other workflows.

2. compute_std_dev() replies to load_timeseries().  That means load_timeseries() needs to accept both a stock as an input or a list of stock prices.  

3. As the workflow becomes more complex load_timeseries() morphs into an orchestration function:

```
@functions.bind('examples/load_timeseries')
def load_timeseries(context, input):

    if isinstance(input, str):
        prices = _load_prices(stock)
        context.send('examples/compute_std_dev', input)
    # elif ... next stage
    # elif ... next stage
    # elif ... next stage etc
    elif isinstance(input, double):  # finally reply to original caller
        context.pack_and_send_egress('topic', input)
```

4. The functions are no longer testable by chaining them together outside of Flink


### Flink Tasks

Flink Tasks wraps up the orchestration function into a Pipeline so that developers can focus on writing simple functions that are combined into workflows using an intuitive API.  As each individual task in a workflow is run as a seperate Flink stateful function invocation, execution is still distributed and can be scaled up as required.

```
tasks = FlinkTasks(
    default_namespace="example", 
    default_worker_name="worker", 
    egress_type_name="example/kafka-generic-egress")


@tasks.bind()
def timeseries_workflow():
    stocks = ['BT.L', 'DAIGn.DE', 'BP.L']

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
def worker(context, task_data: Union[TaskRequest, TaskResult, TaskException]):
    try:
        tasks.run(context, task_data)
    except Exception as e:
        print(f'Error - {e}')
        traceback.print_exc()
```


