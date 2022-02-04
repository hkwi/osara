import unittest
from threading import Event
from osara import Tap, message
from osara.fake import *

tap = Tap()

handle_done = Event()

@tap.handler("test")
def handle_test():
	assert message.value=="test"
	try:
		_ = message.model()
		assert False, "message.model will raise ValueError"
	except ValueError:
		pass
	handle_done.set()

tap._consumer = FakeConsumer(messages=[
	FakeMessage(topic="test",value="test")
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

