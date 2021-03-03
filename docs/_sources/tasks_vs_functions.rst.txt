Tasks vs Functions
==================

Stateful Functions
------------------

In Flink Stateful Functions a stateful function has inputs, outputs, state and has a logical address name of up a namespace, type and id:

.. code-block:: python

    @functions.bind("example/functions")
    def stateful_function(context, input: Union[ProtobufTypeA, ProtobufTypeB, ...]):
        ...

Inputs may be of different types and the outputs include directives to mutate state, call other stateful functions and submit outgoing messages to some egress topic.  
Outputs are not returned from the function but instead written into the context.  The same is true for state:

.. code-block:: python

    @functions.bind("example/functions")
    def stateful_function(context, input: Union[ProtobufTypeA, ProtobufTypeB, ...]):
        state = context['state']
        if not state:
            state = 0

        context['state'] = state + 1
        context.pack_and_send('example/functions/other_function', 'an id', 'an input')
        context.pack_and_send_egress('topic', 'an output')

Functions may also reply to their caller:

.. code-block:: python

    @functions.bind("example/functions")
    def stateful_function(context, input: Union[RequestsType, ResultType, ...]):
        context.pack_and_reply('a response')

which is why a typical Stateful Function signature accepts a Union of types as its input to cater for requests and results.


Comparison with Python Functions
--------------------------------

In Python a plain old function that multiplies two numbers might look like:

.. code-block:: python

    def multiply(x, y):
        return x * y

The corresponding Stateful Function might be:

.. code-block:: python

    @functions.bind("example/functions")
    def stateful_multiply(context, input: TwoNumbersType):
        result = input.x * input.y

        # What to do now?  If I have a caller I should probably reply with the result.  
        # Otherwise maybe I should emit the result on some egress topic?

As Stateful Functions are not fruitful it is not clear how to return the result without knowing how you are going to be
called and by who.  It also has an impact on unit testing:

.. code-block:: python

    result = multiply(3, 2)
    self.assertEqual(6, result)

    stateful_multiply(dummy_context, input)
    self.assertEqual(6, dummy_context...)  # pick through the context directives to find the result.


Flink Tasks
-----------

**Flink Tasks trades the ability to have multiple effects (reply, send, egress) in favour of the simplicty of attributing plain old functions:**

.. code-block:: python

    @tasks.bind()
    def multiply(x, y):
        return x * y

It also allows for arbitrary parameters and return types so long as they can be packed into protobuf by the DefaultSerialiser.
