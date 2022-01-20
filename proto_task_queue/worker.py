# Copyright 2018 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Background workers to run tasks from Cloud Pub/Sub."""

import logging
from typing import Callable, Dict, Generic, Optional, Type, TypeVar

import attr
from proto_task_queue import task_pb2

from google.cloud.pubsub_v1.subscriber import client
from google.cloud.pubsub_v1.subscriber import futures as pubsub_futures
from google.cloud.pubsub_v1.subscriber import message as pubsub_message
from google.protobuf import message as proto_message
from google.protobuf import text_format

# pylint: disable=invalid-name
_TaskArgsType = TypeVar('_TaskArgsType', bound=proto_message.Message)
_TaskArgsClassType = Type[_TaskArgsType]
_TaskCallbackType = Callable[[_TaskArgsType], None]
# pylint: enable=invalid-name


@attr.s(auto_attribs=True)
class _Registration(Generic[_TaskArgsType]):
  """Data about a single registered task type.

  Attributes:
    task_args_class: Proto message class of the task's args.
    callback: Implementation of the task.
  """

  task_args_class: _TaskArgsClassType  # pytype: disable=not-supported-yet
  callback: _TaskCallbackType  # pytype: disable=not-supported-yet


class Worker(object):
  """Background worker that runs tasks from Cloud Pub/Sub.

  Typical usage example:

    # Set up the worker.
    my_worker = Worker()
    my_worker.register(MyProtoClass, my_callback)
    my_worker.register(OtherProtoClass, other_callback)

    # Start subscribing.
    subscribe_future = my_worker.subscribe(
        'projects/my-project/subscriptions/my-subscription')

    # Block the current thread on the subscriber thread.
    subscribe_future.result()

  Alternatively, the last two lines can be replaced with other logic if the
  current thread should continue doing work. In that case, see
  https://docs.python.org/3/library/concurrent.futures.html#future-objects for
  subscribe_future's other methods.
  """

  _message_type_registry: Dict[str, _Registration]
  _subscriber: client.Client
  _possibly_subscribing: bool

  def __init__(
      self,
      pubsub_subscriber_client: Optional[client.Client] = None,
      *,
      task_to_string: Callable[[task_pb2.Task],
                               str] = text_format.MessageToString,
  ):
    """Constructor.

    Args:
      pubsub_subscriber_client: Cloud Pub/Sub subscriber client, or None to use
        the default.
      task_to_string: Function that converts a Task to a human-readable string
        for logging.
    """
    self._message_type_registry = {}
    self._subscriber = pubsub_subscriber_client or client.Client()
    self._possibly_subscribing = False
    self._task_to_string = task_to_string

  def register(self, task_args_class: _TaskArgsClassType,
               callback: _TaskCallbackType) -> None:
    """Registers a new task.

    Calling this method after calling subscribe() is not supported, because the
    internal registry is not thread-safe if one thread writes to it while
    another is reading.

    Args:
      task_args_class: Proto message class of the task's args.
      callback: Implementation of the task. It takes an object of the type
        task_args_class as an argument. This should be idempotent, see
        https://cloud.google.com/pubsub/docs/subscriber

    Raises:
      RuntimeError: register() was called after a call to subscribe().
    """
    if self._possibly_subscribing:
      raise RuntimeError(
          'Worker does not support registering a new task type after a '
          'subscriber is started.')
    full_name = task_args_class.DESCRIPTOR.full_name
    self._message_type_registry[full_name] = _Registration(
        task_args_class=task_args_class, callback=callback)
    logging.info('Registered callback for %s', full_name)

  def subscribe(self,
                subscription_name: str) -> pubsub_futures.StreamingPullFuture:
    """Starts processing tasks from a subscription, in the background.

    Args:
      subscription_name: Relative resource name of the subscription, e.g.,
        "projects/my-project/subscriptions/my-subscription".

    Returns:
      A Future object for the running subscriber.
    """
    self._possibly_subscribing = True
    return self._subscriber.subscribe(subscription_name, self._process_message)

  def _process_message(self, message: pubsub_message.Message) -> None:
    """Processes a single message from Pub/Sub.

    Args:
      message: Message from Pub/Sub.
    """
    # Extract the task proto from the message.
    try:
      task = task_pb2.Task.FromString(message.data)
    except proto_message.DecodeError as e:
      logging.error('Unable to deserialize Task proto: %s', e)
      # If the message is gibberish, nacking keeps putting it back, wasting
      # resources for no reason. If the message is fine but there's a parsing
      # bug, nacking makes it possible to process the message normally after
      # fixing the bug. If the expected format of the message ever changes in an
      # incompatible way and a message with the new format is sent before the
      # worker is updated, nacking makes it possible to process the message
      # normally after updating the worker.
      message.nack()
      return

    # Find the registration, based on the type of proto stored in task.args.
    _, _, full_name = task.args.type_url.partition('/')
    try:
      registration = self._message_type_registry[full_name]
    except KeyError:
      logging.warning('Unknown type of task: %s', task.args.type_url)
      # If the task has a bogus type, nacking keeps putting it back, wasting
      # resources for no reason. If a new task type is added and those tasks are
      # requested before the worker code is updated, nacking makes it possible
      # to process the tasks after the worker code is updated. If an existing
      # task type is removed from the running worker code before all tasks of
      # that type have been processed, nacking keeps putting it back, wasting
      # resources.
      message.nack()
      return

    # Get the args proto.
    args = registration.task_args_class()
    task.args.Unpack(args)

    # Convert the task to a loggable string.
    try:
      task_string = self._task_to_string(task)
    except Exception:  # pylint: disable=broad-except
      logging.exception(
          'Unable to convert task of type %s to a string for logging.',
          full_name)
      # If self._task_to_string() fails for a reason unrelated to the task
      # itself, nacking makes it possible to process the task once
      # self._task_to_string() is working again. If something about the task
      # makes self._task_to_string() fail consistently, nacking makes it
      # possible to process the task once the bug in self._task_to_string() is
      # fixed. Additionally, users can catch and ignore exceptions in
      # self._task_to_string() itself if they want to always process tasks
      # regardless of whether it's possible to log the contents of the task.
      message.nack()
      return

    # Call the registered callback.
    logging.info('Processing task (message_id=%s):\n%s', message.message_id,
                 task_string)
    try:
      registration.callback(args)
    except Exception:  # pylint: disable=broad-except
      logging.exception('Task failed (message_id=%s).', message.message_id)
      # See the comment above about nacking on self._task_to_string() failures
      # for the considerations here.
      message.nack()
    else:
      logging.info('Finished task (message_id=%s).', message.message_id)
      message.ack()
