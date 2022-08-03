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

    if pipeline_proto.inline:
        pipeline = pipeline.inline()

    if state is not None:
        pipeline = pipeline.with_initial(state=state)

    if len(args) > 0:
        pipeline = pipeline.with_initial(args=args)

    return state, pipeline

       
@builtin('__builtins.flatten_results')
def flatten_results(results):
    return [item for items in results for item in items]
