# Proto Task Queue

This is a Python 3 library for managing a task queue over
[Google Cloud Pub/Sub](https://cloud.google.com/pubsub/). Tasks are defined in
[Protocol Buffer](https://developers.google.com/protocol-buffers/) messages, so
the requestor and worker code versions do not need to be kept perfectly in sync.
