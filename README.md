# micro-framewaork on top of kappa architecture
On the top of kappa --- is a disk, called "kappa no sara" in Japanese Yokai.

Syntax was inspired from flask.

```
from osara import Tap

tap = Tap({"bootstrap.servers":"127.0.0.1", "group.id":"demo"})

@tap.handler("topic_x")
def handle_x(msg):
	print("Got %s" % msg)

if __name__=="__main__":
	tap.start().wait()
```
