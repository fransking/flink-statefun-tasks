from statefun_tasks import TaskRequest, TaskResult, TaskException, deserialise_result
from statefun_tasks.client import FlinkTasksClientFactory, TaskError

from aiohttp import web
import json

from .api import greeting_workflow

KAFKA_BROKER = "kafka broker URI"

flink_client = FlinkTasksClientFactory.get_client(KAFKA_BROKER, request_topics={None: 'statefun-test.requests'}, reply_topic=f'statefun-test.reply')

async def index(request):
    try:
        pipeline = greeting_workflow.send('Jane', last_name='Doe')
        result = await flink_client.submit_async(pipeline)

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
