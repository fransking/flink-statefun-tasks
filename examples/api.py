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


# 4. state passing which allows a mutable state variable to be passed between tasks in the workflow

@tasks.bind()
def state_passing_workflow():
    return _store_last_name_length.send('Jane', 'Doe').continue_with(_join_names).continue_with(_do_greeting_with_state)


@tasks.bind(with_state=True)
def _store_last_name_length(initial_state, first_name, last_name):
    initial_state = len(last_name)
    return initial_state, f'{first_name} {last_name}'


@tasks.bind()
def _join_names(first_name, last_name):
    return f'{first_name} {last_name}'


@tasks.bind(with_state=True)
def _do_greeting_with_state(state, full_name):
    return state, f'Hello {full_name}.  Your last name length is {state}'

# 5. finally_do for cleaning up resources


@tasks.bind()
def greeting_workflow_with_cleanup(first_name, last_name):
    return tasks.send(_say_hello, first_name, last_name).continue_with(_say_goodbye, goodbye_message="see you later!") \
        .continue_with(_print_message).finally_do(_cleanup)


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
def _cleanup_with_exception(*args):
    raise Exception('Error cleaning up!')