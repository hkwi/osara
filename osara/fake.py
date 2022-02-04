from confluent_kafka import TopicPartition
from confluent_kafka.admin import ClusterMetadata, TopicMetadata

class FakeMessage(object):
	def __init__(self, topic=None, key=None, value=None):
		self._topic = topic
		self._key = key
		self.payload = value
	
	def topic(self):
		return self._topic
	
	def key(self):
		return self._key

	def value(self):
		return self.payload
	
	def error(self):
		return

class FakeConsumer(object):
	def __init__(self, messages=[]):
		self.topics = set()
		self.messages = messages
	
	def assignment(self):
		return [TopicPartition(t) for t in self.topics]
	
	def list_topics(self, topic=None, timeout=-1):
		return ClusterMetadata(topics={t:TopicMetadata() for t in self.topics})
	
	def subscribe(self, topics, on_assign=None, on_revoke=None, on_lost=None):
		self.topics.update(topics)
		if on_assign:
			on_assign(self, [TopicPartition(t) for t in topics])
	
	def poll(self, timeout=-1):
		if self.messages:
			return self.messages.pop(0)
	
	def commit(self, message=None, offsets=None, asynchronous=True):
		return
