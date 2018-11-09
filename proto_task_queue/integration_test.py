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
"""Integration tests for proto_task_queue's worker and requestor."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import time
import unittest
from unittest import mock
import uuid

from absl.testing import absltest
from proto_task_queue import requestor
from proto_task_queue import test_task_pb2
from proto_task_queue import worker

from google.cloud.pubsub_v1.publisher import client as publisher_client
from google.cloud.pubsub_v1.subscriber import client as subscriber_client

_PROJECT_ID_ENV_VAR = 'PTQ_PROJECT'
_PROJECT_ID = os.environ.get(_PROJECT_ID_ENV_VAR)
_TOPIC_ID = 'ptq-test-{}'.format(uuid.uuid4())
_TOPIC_NAME = 'projects/{}/topics/{}'.format(_PROJECT_ID, _TOPIC_ID)
_SUBSCRIPTION_ID = _TOPIC_ID
_SUBSCRIPTION_NAME = 'projects/{}/subscriptions/{}'.format(
    _PROJECT_ID, _SUBSCRIPTION_ID)


@unittest.skipUnless(
    _PROJECT_ID,
    ('To run integration tests, set {} to a GCP project ID that you can create '
     'topics and subscriptions in.').format(_PROJECT_ID_ENV_VAR))
class ProtoTaskQueueTest(absltest.TestCase):

  def setUp(self):
    self._publisher_client = publisher_client.Client()
    self._publisher_client.create_topic(_TOPIC_NAME)
    self._subscriber_client = subscriber_client.Client()
    self._subscriber_client.create_subscription(_SUBSCRIPTION_NAME, _TOPIC_NAME)

    self._requestor = requestor.Requestor()
    self._worker = worker.Worker()

  def tearDown(self):
    self._subscriber_client.delete_subscription(_SUBSCRIPTION_NAME)
    self._publisher_client.delete_topic(_TOPIC_NAME)

  def test_retry_failure_then_succeed(self):
    # Fail once, then succeed.
    foo_task_processor = mock.Mock(
        side_effect=[RuntimeError('retry this'), None])
    self._worker.register(test_task_pb2.FooTaskArgs, foo_task_processor)
    subscriber_future = self._worker.subscribe(_SUBSCRIPTION_NAME)

    # Request that foo be worked on.
    foo_task_args = test_task_pb2.FooTaskArgs()
    foo_task_args.widget = 'widget maker maker'
    request_future = self._requestor.request(_TOPIC_NAME, foo_task_args)
    request_future.result(timeout=10)

    # Wait up to a minute for the messages to go through, then stop waiting for
    # more.
    for _ in range(60):
      time.sleep(1)
      if foo_task_processor.call_count >= 2:
        break
    subscriber_future.cancel()

    self.assertEqual(2, foo_task_processor.call_count)
    foo_task_processor.assert_has_calls([mock.call(foo_task_args)] * 2)


if __name__ == '__main__':
  absltest.main()
