from ._pipeline import PipelineBuilder


def run_pipeline(pipeline_proto, serialiser):
    return PipelineBuilder.from_proto(pipeline_proto, serialiser)
