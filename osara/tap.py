import logging
import threading
import confluent_kafka
from json import loads as json_loads
from json import dumps as json_dumps
from queue import Queue, Empty
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

@dataclass
class TopicPartition:
	topic: str
	partition: int
	offset: int
	
	class Config:
		orm_mode = True

class Config(dict):
	def __init__(self, both={}, producer={}, consumer={}):
		super().__init__(both)
		self.producer = ChainMap(producer, self)
		self.consumer = ChainMap(consumer, self)


def run_in_thread(init_func, poll_func, daemon=True):
	stop_flag = threading.Event()
	def runner():
		init_func()
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
	
	def __init__(self, config={}):
		self.config = Config(config)
		self._lock = threading.Lock()
		self._schema = {}
		self._handlers = defaultdict(lambda:[])
		self._assigned = []
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
		return run_in_thread(self.poll_prepare, self.poll, daemon=daemon)
	
	def poll_prepare(self, ensure_topics=None):
		with self._lock:
			init = False
			if not self._consumer:
				self._consumer = confluent_kafka.Consumer(dict(self.config.consumer))
				if not self.config.consumer.get("enable.auto.commit", True):
					self._consumer_mutex = threading.Lock()
				init = True
			
			ret = None
			if ensure_topics is not None or init:
				current = {a.topic for a in self._consumer.assignment()}
				topics = self._handlers.keys()
				if ensure_topics is not None:
					ret = threading.Event()
					assert not (set(ensure_topics) - topics), "ensure_topics must be subset of topics"
					if not (set(ensure_topics) - current):
						ret.set()
				if current != set(topics):
					if ret and not ret.is_set():
						def on_assign(consumer, partitions):
							got = {p.topic for p in consumer.assignment() + partitions}
							if ensure_topics is not None and not (set(ensure_topics) - got):
								ret.set()
						
						self._consumer.subscribe(list(topics), on_assign=on_assign)
					else:
						self._consumer.subscribe(list(topics))
			
			return ret
	
	def poll(self):
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
	
	def map_reduce(self, topic, message=None, json=None, topic_filter=[]):
		queue = Queue()
		entry = (lambda m: queue.put(m), {})
		
		for topic in topic_filter:
			self._handlers[topic].append(entry)
		
		ev = self.poll_prepare(ensure_topics=topic_filter)
		while not ev.is_set():
			if self._started:
				ev.wait()
			else:
				self.poll()
		
		if message and isinstance(message, BaseModel):
			json = message.json
		if json:
			message = json_dumps(json)
		if isinstance(message, str):
			message = message.encode("UTF-8")
		
		pcond = Queue()
		producer = confluent_kafka.Producer(dict(self.config.producer))
		producer.produce(topic, message, on_delivery=lambda e,m:pcond.put((e,m)))
		while True:
			try:
				e,m = pcond.get_nowait()
				if e:
					raise e
				break
			except Empty:
				producer.poll(0.1)
		
		class Iter:
			def __iter__(iter_self):
				return iter_self
			
			def __next__(iter_self):
				if not topic_filter:
					raise StopIteration("Use topic_filter to capture response")
				
				if self._started:
					while True:
						try:
							return queue.get(1.0)
						except Empty:
							pass
				else:
					while True:
						try:
							while True:
								return queue.get_nowait()
						except Empty:
							self.poll()
			
			def __del__(iter_self):
				for topic in topic_filter:
					self._handlers[topic].remove(entry)
		return Iter()
