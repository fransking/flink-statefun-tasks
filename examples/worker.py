
from statefun import StatefulFunctions, RequestReplyHandler
import logging
from typing import Union
import traceback

# import FlinkTasks
from statefun_tasks import FlinkTasks, TaskRequest, TaskResult, TaskException, TaskActionRequest, in_parallel
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

from flask import request
from flask import make_response
from flask import Flask

app = Flask(__name__)


@app.route('/statefun', methods=['POST'])
def handle():
    response_data = handler.handle_sync(request.data)
    response = make_response(response_data)
    response.headers.set('Content-Type', 'application/octet-stream')
    return response



if __name__ == "__main__":
    app.run()
