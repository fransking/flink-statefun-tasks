from statefun import StatefulFunctions, AsyncRequestReplyHandler
from aiohttp import web
import asyncio
import json
import logging
from typing import Union
import traceback

# import FlinkTasks
from statefun_tasks import FlinkTasks, TaskRequest, TaskResult, TaskException, in_parallel
from .api import tasks


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


#
# Serve the endpoint
#

async def handle(request):
    handler = AsyncRequestReplyHandler(functions)
    request_data = await request.read()
    response_data = await handler(request_data)
    return web.Response(body=response_data, content_type='application/octet-stream')

async def app():
    web_app = web.Application()
    web_app.router.add_post('/async_worker', handle)
    return web_app
