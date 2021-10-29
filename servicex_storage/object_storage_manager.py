"""
Definition for abstract Object storage manager class
"""


# Copyright (c) 2019, IRIS-HEP
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright notice, this
#   list of conditions and the following disclaimer.
#
# * Redistributions in binary form must reproduce the above copyright notice,
#   this list of conditions and the following disclaimer in the documentation
#   and/or other materials provided with the distribution.
#
# * Neither the name of the copyright holder nor the names of its
#   contributors may be used to endorse or promote products derived from
#   this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

import abc
import pathlib
import typing


class ObjectStore(abc.ABC):
  """
  Abstract class for object storage managers to use
  """
  @abc.abstractmethod
  def get_storage_used(self) -> int:
    """
    Get storage used by object store
    :return: total storage used in bytes
    """

  @abc.abstractmethod
  def upload_file(self, bucket: str, object_name: str, path: pathlib.Path) -> None:
    """
    Save file to object store
    :param bucket: name of bucket
    :param object_name: name of object
    :param path: path to source file
    :return: None
    """

  @abc.abstractmethod
  def cleanup_storage(self, max_size: int) -> (int, typing.List[str]):
    """
    Reduce storage used until it's less than max_size
    :return: Tuple with final storage used and list of buckets removed
    """

  @abc.abstractmethod
  def delete_object(self, bucket: str, object_name: str) -> None:
    """
    Delete object from store
    :param bucket: name of bucket
    :param object_name: name of object
    :return: None
    """

  @abc.abstractmethod
  def get_file(self, bucket: str, object_name: str, path: pathlib.Path) -> None:
    """
    Get an object from store
    :param bucket: name of bucket
    :param object_name: name of object
    :param path: path to destination file (must not be present)
    :return: None
    """
