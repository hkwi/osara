from functools import partial
from werkzeug.local import LocalStack, LocalProxy

_message_ctx_stack = LocalStack()

def _fetch_from_message_ctx(name: str):
	top = _message_ctx_stack.top
	if top is None:
		raise RuntimeError("out of context")
	return getattr(top, name)

raw_message = LocalProxy(partial(_fetch_from_message_ctx, "raw_message"))
message = LocalProxy(partial(_fetch_from_message_ctx, "message"))

class MessageContext(object):
	def __enter__(self):
		_message_ctx_stack.push(self)
		return self
	
	def __exit__(self, exc_type, exc_value, tb):
		_message_ctx_stack.pop()
