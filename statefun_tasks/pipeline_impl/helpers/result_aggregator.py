from statefun_tasks.types import Group, _type_name
from statefun_tasks.context import TaskContext
from statefun_tasks.protobuf import pack_any
from statefun_tasks.messages_pb2 import TaskResults, TaskResult, TaskException, MapOfStringToAny
from collections import deque
import logging


_log = logging.getLogger('FlinkTasks')


class ResultAggregator(object):
    __slots__ = ('_graph', '_serialiser')

    def __init__(self, graph, serialiser):
        self._graph = graph
        self._serialiser = serialiser

    def add_result(self, context: TaskContext, task_uid, task_result_or_exception):
        task_results = context.pipeline_state.task_results  # type: TaskResults

        failed = isinstance(task_result_or_exception, TaskException)
        last_task = self._graph.get_last_task_in_chain(task_uid)
        packed = pack_any(task_result_or_exception)
        
        if task_uid == last_task.uid:
            # if we are the last task in this chain in the group then record this result so we can aggregate laster
            task_results.by_uid[task_uid].CopyFrom(packed)

        elif failed:
            # additionally propagate the error onto the last stage of this chain
            task_results.by_uid[last_task.uid].CopyFrom(packed)

    def aggregate(self, context: TaskContext, group: Group):
        is_top_level = self._graph.is_top_level_group(group)
        task_results = context.pipeline_state.task_results  # type: TaskResults

        aggregated_results, aggregated_states, aggregated_errors = [], [], []
        stack = deque([(group, aggregated_results)])  # FIFO, errors are flat not nested so don't get stacked

        while len(stack) > 0:
            group, results = stack.popleft()
            
            for pipeline in group:
                last_entry = pipeline[-1]

                if isinstance(last_entry, Group):
                    if last_entry.group_id in task_results.by_uid:
                        # if we have already aggregated this group just append the results
                        self._append_results(last_entry.group_id, task_results, results, aggregated_states, aggregated_errors)
                    else:
                        # otherwise iterate over group
                        stack_results = []
                        results.append(stack_results)
                        stack.append((last_entry, stack_results))
                else:
                    # aggregate the group
                    self._append_results(pipeline[-1].uid, task_results, results, aggregated_states, aggregated_errors)


        aggregated_errors = [error for error in aggregated_errors if error is not None]
        aggregated_state = self._aggregate_state(aggregated_states)

        
        if any(aggregated_errors):

            serialised_state = self._serialiser.to_proto(aggregated_state)

            task_result_or_exception = TaskException(            
                id=group.group_id,
                type=f'__aggregate.error',
                exception_type='statefun_tasks.AggregatedError',
                exception_message='|'.join([f'{e.id}, {e.type}, {e.exception_message}' for e in aggregated_errors]),
                stacktrace='|'.join([f'{e.id}, {e.stacktrace}' for e in aggregated_errors]),
                state=pack_any(serialised_state),
                invocation_id=context.pipeline_state.invocation_id)
            
        else:
        
            task_result_or_exception = TaskResult(id=group.group_id, invocation_id=context.pipeline_state.invocation_id)
            self._serialiser.serialise_result(task_result_or_exception, aggregated_results, aggregated_state)

        # save and return the aggregated result
        if not is_top_level:
            task_results.by_uid[group.group_id].CopyFrom(pack_any(task_result_or_exception))

        return task_result_or_exception

    def _append_results(self, uid, task_results, aggregated_results, aggregated_states, aggregated_errors):
        proto = task_results.by_uid[uid]  # Any
        result, state, error = self._unpack_result(proto)

        # We don't need the individual task results anymore so remove to save space / reduce message size
        del task_results.by_uid[uid] 

        aggregated_results.append(result)
        
        aggregated_states.append(state)  # states are flat not nested so don't get stacked
        aggregated_errors.append(error)  # errors are flat not nested so don't get stacked

    @staticmethod
    def _aggregate_state(aggregated_states):
        # if the group tasks all return maps then attempt to flatten the state into a single map
        # symantically equivalent to aggregated_state = dict(ChainMap(*aggregated_states)) if we were using python dicts
        if all([isinstance(state, MapOfStringToAny) for state in aggregated_states]):
            aggregated_state = MapOfStringToAny()
            for key, value in aggregated_states:
                if key not in aggregated_state.items:
                    aggregated_state.items[key] = value

        else: 
            # otherwise return state of the first task in group as we only support state mutation in groups if the state is a mergable dictionary
            aggregated_state = aggregated_states[0]

        return aggregated_state

    def _unpack_result(self, proto):
        result, state, error = None, None, None
        unpacked = self._serialiser.from_proto(proto)
 
        # now split into result, state and error tuple
        if isinstance(unpacked, TaskResult):
            result, state = unpacked.result, unpacked.state
        elif isinstance(unpacked, TaskException):
            error, state = unpacked, unpacked.state

        return result, state, error
