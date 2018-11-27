# python3

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
"""Tests for proto_task_queue.worker."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from unittest import mock
import uuid
import warnings

from absl.testing import absltest
from absl.testing import parameterized
from proto_task_queue import task_pb2
from proto_task_queue import test_task_pb2
from proto_task_queue import worker

from google.cloud.pubsub_v1.subscriber import client
from google.cloud.pubsub_v1.subscriber import message


def _make_task_bytes(args):
  """Returns a serialized Task proto.

  Args:
    args: Proto message to put in the Task's args field.
  """
  task = task_pb2.Task()
  task.args.Pack(args)
  return task.SerializeToString()


def _make_mock_pubsub_message(task_args):
  """Returns a mock pubsub message.

  Args:
    task_args: Proto message to use as args to the task to put in the pubsub
      message.
  """
  msg = mock.create_autospec(message.Message)
  msg.data = _make_task_bytes(task_args)
  msg.message_id = str(uuid.uuid4())
  return msg


class WorkerTest(parameterized.TestCase):

  def setUp(self):
    super().setUp()
    self._client = mock.create_autospec(client.Client)
    self._worker = worker.Worker(pubsub_subscriber_client=self._client)

  def test_routing_and_task_success(self):
    # Register two types of tasks.
    foo_task_processor = mock.Mock()
    bar_task_processor = mock.Mock()
    self._worker.register(test_task_pb2.FooTaskArgs, foo_task_processor)
    self._worker.register(test_task_pb2.BarTaskArgs, bar_task_processor)

    # Create pubsub messages.
    foo_task_args = test_task_pb2.FooTaskArgs(widget=u'Water is made of water.')
    foo_message = _make_mock_pubsub_message(foo_task_args)
    bar_task_args = test_task_pb2.BarTaskArgs(best_number=42)
    bar_message = _make_mock_pubsub_message(bar_task_args)

    # Subscribe, and process the messages.
    subscribe_retval = 'grapes of testing'
    self._mock_subscribe([foo_message, bar_message], retval=subscribe_retval)
    self.assertIs(subscribe_retval, self._worker.subscribe('kumquat'))
    self._client.subscribe.assert_called_once_with('kumquat', mock.ANY)

    # Test that the messages were routed to the correct tasks, and that both
    # messages were ACKed.
    foo_task_processor.assert_called_once_with(foo_task_args)
    foo_message.ack.assert_called_once_with()
    bar_task_processor.assert_called_once_with(bar_task_args)
    bar_message.ack.assert_called_once_with()

  def test_register_after_subscribe_error(self):
    self._worker.subscribe('kumquat')
    with self.assertRaisesRegex(RuntimeError,
                                'register.*after a subscriber is started'):
      self._worker.register(test_task_pb2.FooTaskArgs, mock.Mock())

  @parameterized.named_parameters(
      ('invalid_task_proto', b'this is probably not a valid binary proto'),
      ('unregistered_type', _make_task_bytes(test_task_pb2.FooTaskArgs())))
  def test_unhandled_message(self, message_data):
    self._worker.register(test_task_pb2.BarTaskArgs, mock.Mock())
    msg = mock.create_autospec(message.Message)
    msg.data = message_data
    msg.message_id = str(uuid.uuid4())
    self._mock_subscribe([msg])
    with warnings.catch_warnings():
      # Ignore a warning from trying to parse an invalid binary proto.
      warnings.filterwarnings('ignore', 'Unexpected end-group tag:')
      self._worker.subscribe('kumquat')
    msg.nack.assert_called_once_with()

  def test_callback_error(self):
    foo_task_processor = mock.Mock(side_effect=RuntimeError('foo error'))
    self._worker.register(test_task_pb2.FooTaskArgs, foo_task_processor)
    foo_message = _make_mock_pubsub_message(test_task_pb2.FooTaskArgs())
    self._mock_subscribe([foo_message])
    self._worker.subscribe('kumquat')
    foo_message.nack.assert_called_once_with()

  def _mock_subscribe(self, messages, retval=None):
    """Set up self._client.subscribe() to mock messages being published.

    Note: This processes messages synchronously in the calling thread, unlike a
    real pubsub subscriber.

    Args:
      messages: Iterable of pubsub messages to treat as if they were received
        from pubsub.
      retval: Value to return from subscribe(). Normally that function would
        return a Future, but that isn't needed for this synchronous
        implementation.
    """

    def side_effect(subscription, callback):
      del subscription  # Unused.
      for msg in messages:
        callback(msg)
      return retval

    self._client.subscribe.side_effect = side_effect


if __name__ == '__main__':
  absltest.main()
