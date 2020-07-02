import time
import uuid
import flask
import osara
import flask_sqlalchemy
from pydantic import BaseModel
from typing import Optional
from sqlalchemy import Column, String, Integer
from sqlalchemy.ext.declarative import declarative_base

app = flask.Flask(__name__)
tap = osara.Tap()

PersistModel = declarative_base()
db = flask_sqlalchemy.SQLAlchemy(app, metadata=PersistModel.metadata)
kdb = osara.SQLAlchemy(tap)

@tap.schema("ping")
class Ping(BaseModel):
	id: Optional[str]
	ping: str
	epoch: Optional[int]
	
	class Config:
		orm_mode=True

class PPing(PersistModel):
	__tablename__="ping"
	id = Column(String, default=lambda:str(uuid.uuid4()), primary_key=True)
	ping = Column(String)
	epoch = Column(Integer)

@tap.handler("ping")
def handle_ping(msg):
	data = msg.model()
	data.epoch = int(time.time())
	kdb.session.add(PPing(**dict(data)))
	kdb.session.commit()

@app.route("/", methods=["GET"])
def list_ping():
	return flask.jsonify([Ping.from_orm(p).dict() for p in db.session.query(PPing)])

if __name__=="__main__":
	tap.config.update({"bootstrap.servers":"127.0.0.1", "group.id":"demo_client"})
	kdb.engine = db.engine
	PersistModel.metadata.create_all(db.engine)
	tap.start()
	app.run()
