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
"""Packaging configuration for proto-task-queue."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import subprocess

import setuptools
from setuptools.command import build_py

_PROTO_FILES = [
    'proto_task_queue/task.proto',
    'proto_task_queue/test_task.proto',
]


class BuildProtoAndPy(build_py.build_py):
  """Command to build proto and python files."""

  def run(self):
    """Builds the proto files, then the python files.

    Raises:
      subprocess.CalledProcessError: There was an error compiling proto files.
      Exception: Any exceptions raised by build_py.build_py.run are passed
          through.
    """
    subprocess.run(
        ['protoc', '-I=.', '--python_out=.'] + _PROTO_FILES, check=True)
    super(BuildProtoAndPy, self).run()


setuptools.setup(
    name='proto-task-queue',
    version='0.0.1',
    py_modules=[
        'proto_task_queue.requestor',
        'proto_task_queue.task_pb2',
        'proto_task_queue.worker',
    ],
    python_requires='>=3.6',
    setup_requires=[
        'pytest-runner>=4.2',
    ],
    install_requires=[
        'attrs>=18.2.0',
        'google-cloud-pubsub>=0.38.0',
        'protobuf>=3.6.1',
    ],
    tests_require=[
        'absl-py>=0.6.1',
    ],
    cmdclass={
        'build_py': BuildProtoAndPy,
    },
)
