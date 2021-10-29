from statefun_tasks.types import Group
from statefun_tasks.context import TaskContext
from statefun_tasks.protobuf import pack_any
from statefun_tasks.messages_pb2 import TaskResults, TaskResult, TaskException
from collections import ChainMap, deque


class ResultAggregator(object):
    def __init__(self, graph, serialiser):
        self._graph = graph
        self._serialiser = serialiser

    def add_result(self, context: TaskContext, task_id, task_result_or_exception):
        task_results = context.pipeline_state.task_results  # type: TaskResults

        failed = isinstance(task_result_or_exception, TaskException)

        last_task = self._graph.get_last_task_in_chain(task_id)
        
        if task_id == last_task.task_id:
            # if we are the last task in this chain in the group then record this result so we can aggregate laster
            task_results.by_id[last_task.task_id].CopyFrom(pack_any(task_result_or_exception))

        elif failed:
            # additionally propogate the error onto the last stage of this chain
            task_results.by_id[last_task.task_id].CopyFrom(pack_any(task_result_or_exception))

    def aggregate(self, context: TaskContext, group: Group):
        task_results = context.pipeline_state.task_results  # type: TaskResults

        aggregated_results, aggregated_states, aggregated_errors = [], [], []
        stack = deque([(group, aggregated_results, aggregated_states)])  # FIFO, errors are flat not nested so don't get stacked

        while len(stack) > 0:
            group, results, states = stack.popleft()
            
            for pipeline in group:
                last_entry = pipeline[-1]

                if isinstance(last_entry, Group):
                    stack_results, stack_states = [], []

                    results.append(stack_results)
                    states.append(stack_states)

                    stack.append((last_entry, stack_results, stack_states))
                else:
                    result, state, error = None, {}, None

                    proto = task_results.by_id[pipeline[-1].task_id]  # Any
                    del task_results.by_id[pipeline[-1].task_id]  # We don't need the individual task results anymore so remove to save space / reduce message size

                    task_result_or_exception = self._serialiser.from_proto(proto)

                    if isinstance(task_result_or_exception, TaskResult):
                        result, state = self._serialiser.deserialise_result(task_result_or_exception)
                    elif isinstance(task_result_or_exception, TaskException):
                        error = task_result_or_exception

                    results.append(result)
                    states.append(state)
                    aggregated_errors.append(error)  # errors are flat not nested so don't get stacked

        aggregated_errors = [error for error in aggregated_errors if error is not None]

        if any(aggregated_errors):

            task_exception = TaskException(            
                id=group.group_id,
                type=f'__aggregate.error',
                exception_type='statefun_tasks.AggregatedError',
                exception_message='|'.join([f'{e.id}, {e.type}, {e.exception_message}' for e in aggregated_errors]),
                stacktrace='|'.join([f'{e.id}, {e.stacktrace}' for e in aggregated_errors]))
            
            return task_exception
            
        else:

            # if the group tasks all return dictionaries then attempt to flatten the state into a single dictionary
            if all([isinstance(state, dict) for state in aggregated_states]):
                aggregated_state = dict(ChainMap(*aggregated_states))
            else: 
                # otherwise return state of the first task in group as we only support state mutation in groups if the state is a mergable dictionary
                aggregated_state = aggregated_states[0]

            task_result = TaskResult(id=group.group_id)
            self._serialiser.serialise_result(task_result, aggregated_results, aggregated_state)

            return task_result
