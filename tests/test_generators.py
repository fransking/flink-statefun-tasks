import unittest

from tests.utils import FlinkTestHarness, tasks


@tasks.bind()
async def return_async_generator():
    yield 1
    yield 2
    yield 3


@tasks.bind()
def return_generator():
    yield 4
    yield 5
    yield 6


class GeneratorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.test_harness = FlinkTestHarness()

    def test_async_generator(self):
        pipeline = return_async_generator.send()
        result = self.test_harness.run_pipeline(pipeline)
        self.assertEqual(result, [1, 2, 3])  

    def test_generator(self):
        pipeline = return_generator.send()
        result = self.test_harness.run_pipeline(pipeline)
        self.assertEqual(result, [4, 5, 6])  

if __name__ == '__main__':
    unittest.main()
