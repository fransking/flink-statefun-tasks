from ._pipeline import PipelineBuilder


def run_pipeline(pipeline_proto):
    return PipelineBuilder.from_proto(pipeline_proto)
