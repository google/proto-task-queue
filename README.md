# Proto Task Queue

This is a Python 3 library for managing a task queue over
[Google Cloud Pub/Sub](https://cloud.google.com/pubsub/). Tasks are defined in
[Protocol Buffer](https://developers.google.com/protocol-buffers/) messages, so
the requestor and worker code versions do not need to be kept perfectly in sync.
This is not an officially supported Google product.

In addition to the dependencies listed in `setup.py`, the
[protobuf compiler and well-known types](https://developers.google.com/protocol-buffers/docs/downloads)
(Debian: `sudo apt install protobuf-compiler libprotobuf-dev`) are required.

## Pub/Sub Configuration

While this library can be used with the default configuration for topics and
subscriptions, we recommend setting a 
[retry policy on the Pub/Sub subscription](https://cloud.google.com/pubsub/docs/handling-failures)
that Proto Task Queue is reading messages from. Specifically, we recommend setting
the minimum retry delay to a value greater than 0 so messages aren't immediately
retried after failing for any reason. You may need to customize your retry delay
based on your application's particular messages and latency requirements. If you're
looking for a suggestion, we recommend starting with 60 seconds.
