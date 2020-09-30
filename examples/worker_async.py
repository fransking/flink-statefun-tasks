from statefun import StatefulFunctions
from aiohttp import web
import asyncio
import json
import logging
from typing import Union
import traceback

# import FlinkTasks
from statefun_tasks import FlinkTasks, TaskRequest, TaskResult, TaskException, in_parallel
from .api import tasks
from .async_request_reply import AsyncRequestReplyHandler



logging.basicConfig(level=logging.INFO)

_log = logging.getLogger(__name__)
functions = StatefulFunctions()

_log.info("Worker starting")


@functions.bind("example/async_worker")
async def worker(context, task_data: Union[TaskRequest, TaskResult, TaskException]):
    try:
        await tasks.run_async(context, task_data)
    except Exception as e:
        print(f'Error - {e}')
        traceback.print_exc()


handler = AsyncRequestReplyHandler(functions)

#
# Serve the endpoint
#

async def handle(request):
    request_data = await request.read()
    response_data = await handler.handle_async(request_data)
    return web.Response(body=response_data, content_type='application/octet-stream')

async def app():
    web_app = web.Application()
    web_app.router.add_post('/async_worker', handle)
    return web_app
