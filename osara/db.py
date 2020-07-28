from sqlalchemy import create_engine
from sqlalchemy import orm
from werkzeug.local import LocalProxy
from .g import _message_ctx_stack

def _fetch_session():
	top = _message_ctx_stack.top
	if top is None:
		raise RuntimeError("out of context")
	
	if not hasattr(top, "session"):
		app = top.app
		top.session = session = orm.scoped_session(
			orm.sessionmaker(bind=app._sqlalchemy.engine),
			scopefunc=_message_ctx_stack.__ident_func__)
		
		top.cleanup.append(session.remove)
	
	return top.session

session = LocalProxy(_fetch_session)

class SQLAlchemy(object):
	_engine = None
	
	def __init__(self, app):
		#
		# engine=<some engine from external>
		# SQLALCHEMY_DATABASE_URI=<string>
		#
		app._sqlalchemy = self
		app._sqlalchemy_config = self.config = {}
		self.session = session
	
	@property
	def engine(self):
		if not self._engine:
			self._engine = create_engine(self.config["SQLALCHEMY_DATABASE_URI"])
		return self._engine
	
	@engine.setter
	def engine(self, external):
		self._engine = external
