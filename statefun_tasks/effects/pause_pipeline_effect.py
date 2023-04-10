from statefun_tasks.messages_pb2 import TaskResult
from .effect import Effect


class PausePipelineEffect(Effect):
    def __init__(self, result):
        super().__init__(result)

    @staticmethod
    def apply(task_result: TaskResult):
        task_result.is_wait = True


def with_pause_pipeline(*fn_result):
    return PausePipelineEffect(fn_result)
