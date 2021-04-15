# Flink Tasks
Tasks API for Stateful Functions on Flink

## What is it?

A lightweight API that borrows concepts from Celery to allow Python developers to run task based workflows on Apache Flink Stateful Functions.  Workflows are composed of Tasks which accept parameters and can be chained together as continuations into a Pipeline.  The Pipeline becomes the Flink state.


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


Additional documentation can be found [here](https://fransking.github.io/flink-statefun-tasks).
