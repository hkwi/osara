import datetime
from flask import Flask
from pydantic import BaseModel
from osara import Tap

# To monitor the stream, use
# `kafkacat -b 127.0.0.1 -G demo_monitor demo_ping demo_pong`

app = Flask(__name__)
tap = Tap({"bootstrap.servers":"127.0.0.1","group.id":"demo"})

@tap.schema("demo_ping")
class Ping(BaseModel):
	ping: str

@tap.schema("demo_pong")
class Pong(BaseModel):
	pong: str

@tap.handler("demo_ping")
def handle_demo(msg):
	tap.map_reduce("demo_pong", Pong(pong=msg.model.ping))

@app.route("/", methods=["GET"])
def index():
	for msg in tap.map_reduce("demo_ping", Ping(ping=datetime.datetime.now().isoformat()), topic_filter=["demo_pong"]):
		return msg.model.pong

if __name__=="__main__":
	tap.start()
	app.run()
