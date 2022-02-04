# micro-framewaork on top of kappa architecture
On the top of kappa --- is a disk, called "kappa no sara" in Japanese Yokai.

Syntax was inspired from [faust](https://pypi.org/project/faust/) and [flask](https://pypi.org/project/Flask/).

```
from osara import Tap

tap = Tap({"bootstrap.servers":"127.0.0.1", "group.id":"demo"})

@tap.handler("topic_x")
def handle_x(msg):
	print("Got %s" % msg)

if __name__=="__main__":
	tap.start().wait()
```


## shema support

pydantic schema can be attached to topic.

```python
from pydantic import BaseModel
from osara import Tap
tap = Tap({"bootstrap.servers":"127.0.0.1", "group.id":"demo"})

@tap.schema("topic_x")
class X(BaseModel):
	msg: str

@tap.handler("topic_x")
def handle_x(msg):
	# you can access parsed data via "model" method
	print("Got %s" % msg.model().msg)

if __name__=="__main__":
	tap.start().wait()
```

## produce, then consume

Sometime, we want RPC style message flow.

```
from osara import Tap
tap = Tap({"bootstrap.servers":"127.0.0.1", "group.id":"demo"})
for msg in tap.map_reduce("topic_x", b"hello", topic_filter=["topic_x"]):
	print(msg)
	if msg.value == b"hello":
		break

```


## Notes on flask-sqlalchemy

If you're going to use flask-sqlalchemy Models in
osara handlers, you can use sqlalchemy Session 
with some flask context. Example follows:

```python:web.py
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
app=Flask(__name__)
db=SQLAlchemy(app)

class X(db.Model):
	txt=Column(String, primary_key=True)
```

```python:main.py
from pydantic import BaseModel
from web import app, db, X
from osara import Tap, message

tap = Tap()

@tap.schema("topic_x")
class Xt(BaseModel):
	txt: str

@tap.handler("topic_x")
def handle_x():
	with app.app_context():
		db.session.add(X(**message.model().dict()))
		db.session.commit()
```

If you're not using flask-sqlalchemy, you can
just make use of plane scoped_session.
Some dbapi requires per thread instance, so 
example below use LocalProxy.

```python:combined.py
from uuid import uuid4
from werkzeug.local import Local, LocalProxy
from sqlalchemy import create_engine, Column, String
from sqlalchemy.orm import sessionmaker, scoped_session, declarative_base
from flask import Flask
from osara import Tap

app = Flask(__name__)
tap = Tap()

session=scoped_session(sessionmaker())
Base=declarative_base()

class X(Base):
	__tablename__="x"
	txt=Column(String, primary_key=True)

@app.route("/")
def test():
	session.add(X(txt=f"{uuid4()} from web"))
	session.commit()
	return "OK"

@tap.handler("topic_x")
def x_handler():
	session.add(X(txt=f"{uuid4()} from kfk"))
	session.commit()

l = Local()
def create_engine_proxy(uri):
	def factory():
		if not hasattr(l, uri):
			setattr(l, uri, create_engine(uri))
		return getattr(l, uri)
	return LocalProxy(factory)

if __name__=="__main__":
	engine = create_engine_proxy("sqlite:///hoge.db")
	Base.metadata.create_all(engine)
	session.configure(bind=engine)
	tap.config["group.id"]=str(uuid4())
	tap.config["bootstrap.servers"]="localhost:9092"
	ctl=tap.start()
	try:
		app.run()
	finally:
		ctl.set()
```
