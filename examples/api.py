from statefun_tasks import FlinkTasks, TaskRequest, TaskResult, TaskException, in_parallel, RetryPolicy
from datetime import timedelta
import asyncio

tasks = FlinkTasks(default_namespace="example", default_worker_name="worker",
                   egress_type_name="example/kafka-generic-egress")


# 1. simple workflow

@tasks.bind()
def greeting_workflow(first_name, last_name):
    return _say_hello.send(first_name, last_name).continue_with(_say_goodbye, goodbye_message="see you later!")


@tasks.bind()
def _say_hello(first_name, last_name):
    return f'Hello {first_name} {last_name}'


@tasks.bind(worker_name='async_worker')
async def _say_goodbye(greeting, goodbye_message):
    await asyncio.sleep(5)
    return f'{greeting}.  So now I will say {goodbye_message}'


# 2. throwing exceptions

@tasks.bind()
def error_workflow():
    return _raise_exception.send().continue_with(_wont_be_called)


@tasks.bind(retry_policy=RetryPolicy(delay=timedelta(seconds=5)))
def _raise_exception(first_name, last_name):
    raise ValueError('Workflow will terminate here')


@tasks.bind()
def _wont_be_called():
    print('This should not be called')


# 3. running tasks in parallel followed by a continuation

@tasks.bind()
def parallel_workflow(first_name, last_name):
    return in_parallel([
        _say_hello.send("John", "Smith"),
        _say_hello.send("Jane", "Doe").continue_with(_say_goodbye, goodbye_message="see you later!"),
    ]).continue_with(_count_results)


@tasks.bind()
def _count_results(results):
    return len(results)


# 4. pass through arguments - passing extra parameters through a function: 'f1(a,b) -> c' can be called as 'f1(a,b,1,2...n) -> (c,1,2,...n)
# in the example below the result will be ('Hello Jane Doe', 3)

@tasks.bind()
def passthrough_workflow():
    return _say_hello_and_return_last_name_length.send('Jane', 'Doe').continue_with(_say_goodbye_only)


@tasks.bind()
def _say_hello_and_return_last_name_length(first_name, last_name):
    return f'Hello {first_name} {last_name}', len(last_name)


@tasks.bind()
def _say_goodbye_only(greeting):
    return f'{greeting}'

# 5. finally_do for cleaning up resources


@tasks.bind()
def greeting_workflow_with_cleanup(first_name, last_name):
    return tasks.send(_say_hello, first_name, last_name).continue_with(_say_goodbye, goodbye_message="see you later!") \
        .continue_with(_print_message).finally_do(_cleanup)


@tasks.bind()
def greeting_workflow_with_cleanup_args(first_name, last_name):
    return tasks.send(_say_hello, first_name, last_name).continue_with(_say_goodbye, goodbye_message="see you later!") \
        .continue_with(_print_message).finally_do(_cleanup_with_args, name='Luke')


@tasks.bind()
def greeting_workflow_with_cleanup_error(first_name, last_name):
    return tasks.send(_say_hello, first_name, last_name).continue_with(_say_goodbye, goodbye_message="see you later!") \
        .continue_with(_print_message).finally_do(_cleanup_with_exception)


@tasks.bind()
def greeting_workflow_with_cleanup_and_error(first_name, last_name):
    return tasks.send(_say_hello, first_name, last_name).continue_with(_fail).continue_with(_print_message)\
        .finally_do(_cleanup)


@tasks.bind()
def _fail(greeting):
    raise Exception(f'I am supposed to fail: {greeting}')


@tasks.bind()
def _print_message(greeting):
    print(greeting)
    return greeting


@tasks.bind()
def _cleanup():
    print('cleanup complete')


@tasks.bind()
def _cleanup_with_args(name):
    print(f'cleanup complete for name {name}')


@tasks.bind()
def _cleanup_with_exception():
    raise Exception('Error cleaning up!')