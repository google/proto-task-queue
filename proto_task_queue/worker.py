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

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
from typing import Callable, Dict, Generic, Optional, Text, Type, TypeVar

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

  task_args_class: _TaskArgsClassType
  callback: _TaskCallbackType


class Worker(object):
  """Background worker that runs tasks from Cloud Pub/Sub."""

  _message_type_registry: Dict[Text, _Registration]
  _subscriber: client.Client
  _possibly_subscribing: bool

  def __init__(self, pubsub_subscriber_client: Optional[client.Client] = None):
    """Constructor.

    Args:
      pubsub_subscriber_client: Cloud Pub/Sub subscriber client, or None to use
        the default.
    """
    self._message_type_registry = {}
    self._subscriber = pubsub_subscriber_client or client.Client()
    self._possibly_subscribing = False

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
                subscription_name: Text) -> pubsub_futures.StreamingPullFuture:
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
      message.nack()
      return

    # Find the registration, based on the type of proto stored in task.args.
    _, _, full_name = task.args.type_url.partition('/')
    try:
      registration = self._message_type_registry[full_name]
    except KeyError:
      logging.warning('Unknown type of task: %s', task.args.type_url)
      message.nack()
      return

    # Get the args proto.
    args = registration.task_args_class()
    task.args.Unpack(args)

    # Call the registered callback.
    logging.info('Processing task (message_id=%s): %s', message.message_id,
                 text_format.MessageToString(task))
    try:
      registration.callback(args)
    except Exception:  # pylint: disable=broad-except
      logging.exception('Task failed (message_id=%s).', message.message_id)
      message.nack()
    else:
      logging.info('Finished task (message_id=%s).', message.message_id)
      message.ack()
