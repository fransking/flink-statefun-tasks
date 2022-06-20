from statefun_tasks.pipeline_builder import PipelineBuilder


def run_pipeline(pipeline_proto):
    return PipelineBuilder.from_proto(pipeline_proto).validate()
