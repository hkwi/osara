from functools import partial
from werkzeug.local import LocalStack, LocalProxy

_message_ctx_stack = LocalStack()

def _fetch_from_message_ctx(name: str):
	top = _message_ctx_stack.top
	if top is None:
		raise RuntimeError("out of context")
	return getattr(top, name)

raw_message = LocalProxy(partial(_fetch_from_message_ctx, "raw_message"))

class Context(object):
	def __init__(self, app):
		self.app = app
		self.cleanup = []
	
	def __enter__(self):
		_message_ctx_stack.push(self)
		return self
	
	def __exit__(self, exc_type, exc_value, tb):
		for func in self.cleanup:
			func()
		_message_ctx_stack.pop()
