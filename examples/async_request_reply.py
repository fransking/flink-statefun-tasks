################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

# Modified RequestReplyHandler to enable async functions

from datetime import timedelta

from google.protobuf.any_pb2 import Any

from statefun.core import SdkAddress
from statefun.core import StatefulFunction
from statefun.core import AnyStateHandle
from statefun.core import parse_typename

from statefun import RequestReplyHandler
from statefun.request_reply import BatchContext

# generated function protocol
from statefun.request_reply_pb2 import FromFunction
from statefun.request_reply_pb2 import ToFunction


class AsyncRequestReplyHandler(RequestReplyHandler):
    def __init__(self, functions):
        self.functions = functions

    async def handle_async(self, request_bytes):
        request = ToFunction()
        request.ParseFromString(request_bytes)
        reply = await self.handle_invocation_async(request)
        return reply.SerializeToString()

    async def handle_invocation_async(self, to_function):
        #
        # setup
        #
        context = BatchContext(to_function.invocation.target, to_function.invocation.state)
        target_function = self.functions.for_type(context.address.namespace, context.address.type)
        if target_function is None:
            raise ValueError("Unable to find a function of type ", target_function)
        #
        # process the batch
        #
        batch = to_function.invocation.invocations
        await self.invoke_batch_async(batch, context, target_function)
        #
        # prepare an invocation result that represents the batch
        #
        from_function = FromFunction()
        invocation_result = from_function.invocation_result
        self.add_mutations(context, invocation_result)
        self.add_outgoing_messages(context, invocation_result)
        self.add_delayed_messages(context, invocation_result)
        self.add_egress(context, invocation_result)
        return from_function

    @staticmethod
    async def invoke_batch_async(batch, context, target_function: StatefulFunction):
        fun = target_function.func
        for invocation in batch:
            context.prepare(invocation)
            unpacked = target_function.unpack_any(invocation.argument)
            if not unpacked:
                await fun(context, invocation.argument)
            else:
                await fun(context, unpacked)
