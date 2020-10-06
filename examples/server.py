from statefun_tasks import TaskRequest, TaskResult, TaskException, deserialise_result
from statefun_tasks.client import FlinkTasksClient, TaskError

from aiohttp import web
import socket
import json

from .api import greeting_workflow

KAFKA_BROKER = "kafka broker URI"

flink_client = FlinkTasksClient(KAFKA_BROKER, topic='statefun-test.requests', reply_topic=f'statefun-test.reply.{socket.gethostname()}')

async def index(request):
    try:
        result = await flink_client.send_async(greeting_workflow, 'Jane', last_name='Doe')

        response_data = {
            'result': result
        }
    except Exception as ex:
        response_data = {'error': str(ex)}

    return web.Response(text=json.dumps(response_data, indent=True), content_type='application/json')


async def app():
    web_app = web.Application()
    web_app.router.add_get('/', index)
    return web_app
