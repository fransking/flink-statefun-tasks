from statefun_tasks.pipeline_builder import PipelineBuilder
from statefun_tasks.context import TaskContext
from statefun_tasks.builtin import builtin


@builtin('__builtins.run_pipeline')
def run_pipeline(context: TaskContext, state, *args):
    pipeline_proto = args[-1]
    args = args[:-1]

    pipeline = PipelineBuilder \
        .from_proto(pipeline_proto) \
        .set_task_defaults(default_namespace=context.get_namespace(), default_worker_name=context.get_worker_name()) \
        .validate()

    # if the pipeline is inline then pass in the args to this task and state
    if pipeline_proto.inline:
        pipeline = pipeline.inline()

    pipeline = pipeline.with_initial(args, state)

    return state, pipeline

       
@builtin('__builtins.flatten_results')
def flatten_results(results):
    return [item for items in results for item in items]
