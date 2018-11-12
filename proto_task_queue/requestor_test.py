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
"""Tests for proto_task_queue.requestor."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from unittest import mock

from absl.testing import absltest
from absl.testing import parameterized
from proto_task_queue import requestor
from proto_task_queue import task_pb2
from proto_task_queue import test_task_pb2

from google.cloud.pubsub_v1.publisher import client


class RequestorTest(parameterized.TestCase):

  def setUp(self):
    self._client = mock.create_autospec(client.Client)
    self._requestor = requestor.Requestor(pubsub_publisher_client=self._client)

  @parameterized.named_parameters(('request', True), ('request_task', False))
  def test_publish_is_called_by(self, request_by_args):
    args = test_task_pb2.FooTaskArgs()
    task = task_pb2.Task()
    task.args.Pack(args)
    publish_future = mock.Mock()
    self._client.publish.return_value = publish_future
    if request_by_args:
      returned_future = self._requestor.request('kumquat', args)
    else:
      returned_future = self._requestor.request_task('kumquat', task)
    self.assertIs(publish_future, returned_future)
    self._client.publish.assert_called_once_with('kumquat',
                                                 task.SerializeToString())


if __name__ == '__main__':
  absltest.main()
