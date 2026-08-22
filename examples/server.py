from statefun_tasks.client import FlinkTasksClientFactory

from fastapi import FastAPI
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse

from .api import greeting_workflow

KAFKA_BROKER = "kafka broker URI"

flink_client = FlinkTasksClientFactory.get_client(
    KAFKA_BROKER, 
    request_topics={None: 'statefun-test.requests'}, 
    action_topics={None: 'statefun-test.actions'}, 
    reply_topic=f'statefun-test.reply',
    kafka_producer_properties={'compression_type': 'gzip'})

app = FastAPI()
app.add_middleware(GZipMiddleware)


@app.get("/")
async def index():
    try:
        pipeline = greeting_workflow.send('Jane', last_name='Doe')
        result = await flink_client.submit_async(pipeline)

        response_data = {
            'result': result
        }
    except Exception as ex:
        response_data = {'error': str(ex)}

    return JSONResponse(content=response_data)
