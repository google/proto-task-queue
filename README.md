# Proto Task Queue

This is a Python 3 library for managing a task queue over
[Google Cloud Pub/Sub](https://cloud.google.com/pubsub/). Tasks are defined in
[Protocol Buffer](https://developers.google.com/protocol-buffers/) messages, so
the requestor and worker code versions do not need to be kept perfectly in sync.

In addition to the dependencies listed in `setup.py`, the
[protobuf compiler and well-known types](https://developers.google.com/protocol-buffers/docs/downloads)
(Debian: `sudo apt install protobuf-compiler libprotobuf-dev`) are required.
