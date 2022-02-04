import unittest
from threading import Event
from pydantic import BaseModel
from osara import Tap, message
from osara.fake import *

tap = Tap()
handle_done = Event()

@tap.schema("test")
class Test(BaseModel):
	txt: str

@tap.handler("test")
def handle_test():
	obj = message.model()
	assert obj.txt=="test"
	handle_done.set()

tap._consumer = FakeConsumer(messages=[
	FakeMessage(topic="test",value='{"txt":"test"}')
])

class TestModel(unittest.TestCase):
	def test_consume(self):
		ctl = tap.start()
		handle_done.wait(1)
		ctl.set()
		assert handle_done.is_set()
		assert not tap._consumer.messages

if __name__ == "__main__":
	unittest.main()

