import logging
import threading
import functools
import confluent_kafka
from datetime import datetime
from json import loads as json_loads
from json import dumps as json_dumps
from queue import Queue, Empty
from pydantic import BaseModel
from dataclasses import dataclass
from collections import defaultdict, ChainMap
from .g import _message_ctx_stack, Context

logger = logging.getLogger(__name__)

def format_partitions(ps):
	topics = {}
	errors = []
	for p in ps:
		if p.error:
			errors.append(p)
		elif p.topic in topics:
			topics[p.topic].append(p.partition)
		else:
			topics[p.topic] = [p.partition]
	
	if errors:
		topic["errors"] = errors
	
	return topics

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
	
	#
	# Styles to access converted format
	#
	# - requests.Response#json
	# - flask.Request#get_json
	# - pydantic.BaseModel#dict
	# - pydantic.BaseModel#json
	#
	# `request.json` deprecation discussion
	# https://github.com/pallets/flask/issues/1421
	#
	
	def model(self):
		if self.schema:
			return self.schema.parse_raw(self.value)
		else:
			raise ValueError("No model schema attached")
	
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


def run_in_thread(poll_func, daemon=True):
	stop_flag = threading.Event()
	def runner():
		while not stop_flag.is_set():
			poll_func()
	
	th = threading.Thread(target=runner)
	th.daemon = daemon
	th.start()
	
	return stop_flag

class NotReady(Exception):
	pass

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
	_producer = None
	
	def __init__(self, config={}):
		self.config = Config(config)
		self._lock = threading.Lock()
		self._schema = {}
		self._handlers = defaultdict(lambda:[])
		self._assigned = []
		self._error_handlers = []
		self.on_assign_cb_list = []
	
	def schema(self, topic_name):
		def wrapper(cls):
			self._schema[topic_name] = cls
			return cls
		return wrapper
	
	def handler(self, topic_name, **opts):
		def wrapper(func):
			new_topic = not self._handlers[topic_name]
			self._handlers[topic_name].append((func, opts))
			return func
		return wrapper
	
	def error_handler(self):
		def wrapper(func):
			self._error_handlers.append(func)
			return func
		return wrapper
	
	def context(self):
		return Context(self)
	
	def start(self, daemon=True, init_timestamp=None):
		self._started = True
		init = self.poll_prepare(timestamp=init_timestamp)
		
		ctrl = run_in_thread(self.poll, daemon=daemon)
		
		while True:
			init.wait(1.0)
			if init.is_set():
				break
		
		return ctrl
	
	def on_assign(self, consumer, partitions):
		logger.warn("on_assign %s + %s",
			format_partitions(consumer.assignment()),
			format_partitions(partitions))
		ok = []
		for cb in self.on_assign_cb_list:
			try:
				cb(consumer, partitions)
				ok.append(cb)
			except NotReady:
				pass
			except:
				logger.error("on_assign failed %s" % cb, exc_info=True)
		
		for cb in ok:
			self.on_assign_cb_list.remove(cb)
	
	def on_revoke(self, consumer, partitions):
		logger.warn("on_revoke %s + %s",
			format_partitions(consumer.assignment()),
			format_partitions(partitions))
		ok = []
		for cb in self.on_assign_cb_list:
			try:
				cb(consumer, partitions)
				ok.append(cb)
			except:
				pass
		
		for cb in ok:
			self.on_assign_cb_list.remove(cb)
	
	def poll_prepare(self, ensure_topics=None, timestamp=None):
		ret = threading.Event()
		with self._lock:
			init = False
			if not self._consumer:
				self._consumer = confluent_kafka.Consumer(dict(self.config.consumer))
				if not self.config.consumer.get("enable.auto.commit", True):
					# producer shall barrier consumer commit in transactional api
					self._consumer_mutex = threading.Lock()
				init = True
			
			seek_topics = set()
			if timestamp:
				if isinstance(timestamp, datetime):
					timestamp = int(timestamp.timestamp()*1000)
				
				if ensure_topics:
					seek_topics = set(ensure_topics)
				else:
					seek_topics = set(self._handlers.keys())
			
			def on_assign(consumer, partitions):
				ready = True
				current = {a.topic for a in self._consumer.assignment()}
				current.update({p.topic for p in partitions})
				if ensure_topics and set(ensure_topics) - current:
					ready = False
				
				seeks = []
				for p in partitions:
					if p.topic in seek_topics:
						seeks.append(confluent_kafka.TopicPartition(p.topic, p.partition, timestamp))
				
				if seeks:
					consumer.assign(consumer.offsets_for_times(seeks))
					seek_topics.difference_update({p.topic for p in seeks})
				
				if seek_topics:
					ready = False
				
				if ready:
					ret.set()
				else:
					raise NotReady()
			
			topics = set(self._handlers.keys())
			if ensure_topics:
				topics.update(ensure_topics)
			self.on_assign_cb_list.append(on_assign)
			
			current = {p.topic for p in self._consumer.assignment()}
			if topics - current:
				self._consumer.subscribe(list(topics), on_assign=self.on_assign)
			else:
				ret.set()
		
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
		return MapReduce(
			tap=self, topic_filter=topic_filter
		).map(
			topic, message=message, json=json
		).reduce()


class MapReduce:
	_capture = None
	_cb_entry = None
	
	def __init__(self, tap, topic_filter=[]):
		self.tap = tap
		self.topic_filter = topic_filter
		
		self._reduce = q = Queue()
		self._cb_entry = (lambda m: q.put(m), {})
		for t in topic_filter:
			self.tap._handlers[t].append(self._cb_entry)
		
		ev = self.tap.poll_prepare(ensure_topics=topic_filter)
		if ev:
			while not ev.is_set():
				if self.tap._started:
					ev.wait()
				else:
					self.tap.poll()
	
	def map(self, topic, message=None, json=None):
		if message and isinstance(message, BaseModel):
			json = message.dict()
		if json:
			message = json_dumps(json)
		if isinstance(message, str):
			message = message.encode("UTF-8")
		
		pcond = Queue()
		if self.tap._producer is None:
			self.tap._producer = confluent_kafka.Producer(dict(self.tap.config.producer))
		self.tap._producer.produce(topic, message, on_delivery=lambda e,m:pcond.put((e,m)))
		while True:
			try:
				e,m = pcond.get_nowait()
				if e:
					raise e
				break
			except Empty:
				self.tap._producer.poll(0.1)
		return self
	
	def reduce(self):
		if self.topic_filter:
			if self.tap._started:
				while True:
					try:
						yield self._reduce.get(1.0)
					except Empty:
						pass
			else:
				while True:
					try:
						while True:
							yield self._reduce.get_nowait()
					except Empty:
						self.tap.poll()
	
	def __del__(self):
		for t in self.topic_filter:
			self.tap._handlers[t].remove(self._cb_entry)
