from statefun_tasks import FlinkTasks, TaskRequest, TaskResult, TaskException, in_parallel


tasks = FlinkTasks(default_namespace="example", default_worker_name="worker", egress_type_name="example/kafka-generic-egress")


@tasks.bind()
def greeting_workflow(first_name, last_name):
    return _say_hello.send(first_name, last_name).continue_with(_say_goodbye, goodbye_message="see you later!")


@tasks.bind()
def _say_hello(first_name, last_name):
    return f'Hello {first_name} {last_name}'


@tasks.bind()
def _say_goodbye(greeting, goodbye_message):
    return f'{greeting}.  So now I will say {goodbye_message}'
