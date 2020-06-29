import logging
import threading
import confluent_kafka
from json import loads as json_loads
from json import dumps as json_dumps
from queue import Queue
from pydantic import BaseModel
from dataclasses import dataclass
from collections import defaultdict, ChainMap
from .g import _message_ctx_stack, Context

#
# from confluent_kafka view, splitting Message to
# 1. error
# 2. access metadata(partition)
# 3. message
#
@dataclass
class Message:
	key: bytes
	value: bytes
	schema: BaseModel
	
	@property
	def model(self):
		if self.schema:
			return self.schema.parse_raw(self.value)
		else:
			raise ValueError("No model schema attached")
	
	@property
	def json(self):
		return json_loads(self.value)


class Config(dict):
	def __init__(self, both={}, producer={}, consumer={}):
		super().__init__(both)
		self.producer = ChainMap(producer, self)
		self.consumer = ChainMap(consumer, self)


def run_in_thread(poll_func, daemon=True):
	stop_flag = threading.Event()
	def runner():
		while not stop_flag.is_set():
			poll_func()
	
	th = threading.Thread(target=runner)
	th.daemon = daemon
	th.start()
	return stop_flag

#
# Plans to make kafka runtime pluggable
# - async
#   - aiokafka
#   - aiohttp + rest_proxy
# - sync
#   - confluent_kafka
#   - kafka-python
#   - rest_proxy

class Tap(object):
	_started = False
	_consumer = None
	_consumer_mutex = None
	
	def __init__(self):
		self.config = {}
		self._schema = {}
		self._handlers = defaultdict(lambda:[])
		self._error_handlers = []
	
	def schema(self, topic_name):
		def wrapper(cls):
			self._schema[topic_name] = cls
		return wrapper
	
	def handler(self, topic_name, **opts):
		def wrapper(func):
			new_topic = not self._handlers[topic_name]
			self._handlers[topic_name].append((func, opts))
			if new_topic and self._consumer:
				self._consumer.subscribe(list(self._handlers.keys()))
		return wrapper
	
	def error_handler(self):
		def wrapper(func):
			self._error_handlers.append(func)
		return wrapper
	
	def context(self):
		return Context(self)
	
	def start(self, daemon=True):
		self._started = True
		return run_in_thread(self.poll, daemon=daemon)
	
	def poll(self):
		if self._consumer is None:
			topics = list(self._handlers.keys())
			
			consumer = confluent_kafka.Consumer(self.config.consumer)
			consumer.subscribe(topics)
			self._consumer = consumer
			
			if not self.config.consumer.get("enable.auto.commit", True):
				self._consumer_mutex = threading.Lock()
		
		msg = self._consumer.poll(0.1)
		if msg is None:
			return
		
		with self.context() as ctx:
			ctx.raw_message = msg
			if msg.error():
				for func in self._error_handlers:
					try:
						func()
					except:
						logging.error("error_handler failed", exc_info=True)
			else:
				topic = msg.topic()
				for func,opts in self._handlers[topic]:
					schema = opts.get("schema", self._schema.get(topic))
					m = Message(
						key=msg.key(),
						value=msg.value(),
						schema=schema
					)
					try:
						func(m)
					except:
						logging.error("handler failed", exc_info=True)
		
		if self._consumer_mutex:
			with self._consumer_mutex:
				self._consumer.commit(msg)
	
	def map_reduce(topic, message=None, json=None, topic_filter=[]):
		queue = Queue()
		entry = (lambda m: queue.put(m), {})
		try:
			for topic in topic_filter:
				tap._handlers[topic].append(entry)
			
			if message and isinstance(message, BaseModel):
				json = message.json
			if json:
				message = json_dumps(json)
			if isinstance(message, str):
				message = message.encode("UTF-8")
			
			if self._producer is None:
				self._producer = confluent_kafka.Producer(self.config.producer)
			
			self._producer.produce(topic, message, )
			
			if self._started:
				for q in queue:
					yield q
			else:
				while True:
					for q in queue:
						yield q
					self.poll()
		finally:
			for topic in topic_filter:
				tap._handlers[topic].remove(entry)
