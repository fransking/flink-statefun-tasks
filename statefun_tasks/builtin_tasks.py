from statefun_tasks.context import TaskContext
from statefun_tasks.pipeline_builder import PipelineBuilder


def run_pipeline(context: TaskContext, pipeline_proto):
    return PipelineBuilder \
        .from_proto(pipeline_proto) \
        .set_task_defaults(default_namespace=context.get_namespace(), default_worker_name=context.get_worker_name()) \
        .validate()


def flatten_results(results):
    return [item for items in results for item in items]
