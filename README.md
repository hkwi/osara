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

```
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

