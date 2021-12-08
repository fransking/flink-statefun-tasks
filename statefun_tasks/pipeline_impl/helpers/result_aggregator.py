from statefun_tasks.storage import StorageBackend
from statefun_tasks.types import Group, _type_name
from statefun_tasks.context import TaskContext
from statefun_tasks.protobuf import pack_any
from statefun_tasks.messages_pb2 import TaskResults, TaskResult, TaskException, MapOfStringToAny
from google.protobuf.wrappers_pb2 import StringValue
from google.protobuf.any_pb2 import Any
from collections import deque
import traceback as tb
import logging


_log = logging.getLogger('FlinkTasks')


class ResultAggregator(object):
    def __init__(self, graph, serialiser, storage: StorageBackend):
        self._graph = graph
        self._serialiser = serialiser
        self._storage = storage

    async def add_result(self, context: TaskContext, group: Group, task_id, task_result_or_exception):
        task_results = context.pipeline_state.task_results  # type: TaskResults

        failed = isinstance(task_result_or_exception, TaskException)
        last_task = self._graph.get_last_task_in_chain(task_id)
        packed = pack_any(task_result_or_exception)
        
        if task_id == last_task.task_id:
            # if we are the last task in this chain in the group then record this result so we can aggregate laster
            await self._save_result(group, task_id, packed, task_results, context.pipeline_state_size)

        elif failed:
            # additionally propagate the error onto the last stage of this chain
            await self._save_result(group, last_task.task_id, packed, task_results, context.pipeline_state_size)

    async def aggregate(self, context: TaskContext, group: Group):
        task_results = context.pipeline_state.task_results  # type: TaskResults

        aggregated_results, aggregated_states, aggregated_errors = [], [], []
        stack = deque([(group, aggregated_results)])  # FIFO, errors are flat not nested so don't get stacked

        while len(stack) > 0:
            group, results = stack.popleft()
            
            for pipeline in group:
                last_entry = pipeline[-1]

                if isinstance(last_entry, Group):
                    stack_results = []
                    results.append(stack_results)
                    stack.append((last_entry, stack_results))
                else:
                    proto = task_results.by_id[pipeline[-1].task_id]  # Any
                    result, state, error = await self._load_result(group, proto)

                    # We don't need the individual task results anymore so remove to save space / reduce message size
                    del task_results.by_id[pipeline[-1].task_id] 

                    results.append(result)
                    
                    aggregated_states.append(state)  # states are flat not nested so don't get stacked
                    aggregated_errors.append(error)  # errors are flat not nested so don't get stacked

        # cleanup storage
        await self._cleanup_storage(group)

        aggregated_errors = [error for error in aggregated_errors if error is not None]
        aggregated_state = self._aggregate_state(aggregated_states)

        if any(aggregated_errors):

            serialised_state = self._serialiser.to_proto(aggregated_state)

            task_exception = TaskException(            
                id=group.group_id,
                type=f'__aggregate.error',
                exception_type='statefun_tasks.AggregatedError',
                exception_message='|'.join([f'{e.id}, {e.type}, {e.exception_message}' for e in aggregated_errors]),
                stacktrace='|'.join([f'{e.id}, {e.stacktrace}' for e in aggregated_errors]),
                state=pack_any(serialised_state))
            
            return task_exception
            
        else:
        
            task_result = TaskResult(id=group.group_id)
            self._serialiser.serialise_result(task_result, aggregated_results, aggregated_state)

            return task_result
    
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

    async def _save_result(self, group, task_id, proto, task_results, size_of_state):
        saved_to_storage = False

        if self._storage is not None and size_of_state >= self._storage.threshold:
            saved_to_storage = await self._try_save_to_store([group.group_id, task_id], proto)

        if saved_to_storage: 
            # record ptr to state
            task_results.by_id[task_id].CopyFrom(pack_any(StringValue(value=task_id)))
        else:
            # record data to state
            task_results.by_id[task_id].CopyFrom(proto)

    async def _try_save_to_store(self, keys, proto):
        try:
            await self._storage.store(keys, proto.SerializeToString())
            return True
        except Exception as ex:
            _log.warning(f'Error saving {keys} to backend storage - {ex}')
            return False

    async def _load_result(self, group, proto):
        result, state, error = None, None, None
        unpacked = self._serialiser.from_proto(proto)
 
        if isinstance(unpacked, str):  # load from store
            task_id = unpacked
            try:
                if self._storage is None:
                    raise ValueError('Missing storage backend')

                packed_bytes = await self._storage.fetch([group.group_id, task_id])
                packed = Any()
                packed.ParseFromString(packed_bytes)
                unpacked = self._serialiser.from_proto(packed)

            except Exception as ex:
                _log.error(f'Error loading {task_id} from backend storage - {ex}')

                unpacked = TaskException(            
                    id=task_id,
                    type=f'__storage_backend.error',
                    exception_type=_type_name(ex),
                    exception_message=str(ex),
                    stacktrace=tb.format_exc())

        # now split into result, state and error tuple
        if isinstance(unpacked, TaskResult):
            result, state = unpacked.result, unpacked.state
        elif isinstance(unpacked, TaskException):
            error, state = unpacked, unpacked.state

        return result, state, error

    async def _cleanup_storage(self, group):
        if self._storage is None:
            return

        try:
            await self._storage.delete([group.group_id])
        except Exception as ex:
            _log.warning(f'Error cleaning up results for group {group.group_id} from backend storage - {ex}')
