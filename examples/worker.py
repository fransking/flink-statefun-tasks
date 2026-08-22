
from statefun_tasks.core.statefun import StatefulFunctions, RequestReplyHandler
from fastapi import FastAPI, Request
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import Response
import logging
import traceback
from .api import tasks

logging.basicConfig(level=logging.INFO)


_log = logging.getLogger(__name__)
_log.info("Worker starting")


functions = StatefulFunctions()


@functions.bind("example/worker", specs=tasks.value_specs())
async def worker(context, message):
    try:
        await tasks.run_async(context, message)
    except Exception as e:
        print(f'Error - {e}')
        traceback.print_exc()


handler = RequestReplyHandler(functions)

#
# Serve the endpoint
#

app = FastAPI()
app.add_middleware(GZipMiddleware)


@app.post("/statefun")
async def handle(request: Request):
    data = await request.body()
    response_data = handler.handle_sync(data)
    return Response(
        content=response_data,
        media_type='application/octet-stream',
    )
