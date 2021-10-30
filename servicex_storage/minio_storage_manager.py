"""
Implementation of storage manager for minio based storage
"""

import datetime
import logging
import os
import pathlib
import concurrent.futures
import typing
from typing import List
from collections import namedtuple

import minio
from minio.deleteobjects import DeleteObject

from servicex_storage import object_storage_manager

BucketInfo = namedtuple('BucketInfo', ['name', 'size', 'last_modified'])


class MinioStore(object_storage_manager.ObjectStore):
  """
  Class to handle operations for minio storage
  """

  def __init__(self, minio_url: str, access_key: str, secret_key: str):
    super().__init__()

    self.logger = logging.getLogger(__name__)
    self.logger.addHandler(logging.NullHandler())

    self.minio_url = minio_url
    self.access_key = access_key
    self.secret_key = secret_key

    # minio client is thread safe using Threading, not so much with multiprocessing
    self.__minio_client = minio.Minio(self.minio_url,
                                      access_key=self.access_key,
                                      secret_key=self.secret_key)

  def get_bucket_info(self, bucket: str) -> BucketInfo:
    """
    Given a bucket, get the size and last modified date

    :param bucket: bucket name
    :return: None
    """

    objects = self.__minio_client.list_objects(bucket)
    size = 0
    last_modified = datetime.datetime.now()
    for obj in objects:
      result = self.__minio_client.stat_object(bucket, obj)
      size += result.size
      if result.last_modified < last_modified:
        last_modified = result.last_modified
    return BucketInfo(name=bucket, size=size, last_modified=last_modified)

  def delete_bucket(self, bucket: str) -> bool:
    """
    Delete a given bucket and contents from minio

    :param bucket: bucket name
    :return:  None
    """
    if not self.__minio_client.bucket_exists(bucket):
      return True
    objects = self.__minio_client.list_objects(bucket)
    errors = self.__minio_client.remove_objects(bucket, objects)
    if len(errors) != 0:
      return False
    self.__minio_client.remove_bucket(bucket)
    return True

  def get_storage_used(self) -> int:
    """
    Get the number of bytes used

    :return: integer with number of bytes used
    """
    buckets = self.__minio_client.list_buckets()
    if len(buckets) == 0:
      return 0

    if "THREADS" in os.environ:
      try:
        threads = int(os.environ["THREADS"])
        self.logger.debug("Using %d threads for storage calculation", threads)
      except ValueError:
        self.logger.exception("THREADS env variable not a number, using a single thread")
        threads = 1
    else:
      self.logger.debug("Using a single thread for storage calculation")
      threads = 1

    sizes = []
    # must use ThreadPool since minio client is thread safe with threading only
    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
      sizes = executor.map(lambda x: self.get_bucket_info(x).size, buckets)
    total_size = sum(sizes)
    return total_size

  def delete_object(self, bucket: str, object_name: str) -> None:
    """
    Remove object from minio storage
    :param bucket:  name of bucket
    :param object_name:  name of object
    :return: None
    """
    self.__minio_client.remove_object(bucket, object_name)

  def delete_objects(self, bucket: str, object_names: List[str]) -> List[(str, str)]:
    """
    Delete object from store
    :param bucket: name of bucket
    :param object_names: name of object
    :return: List of tuples (objectName, error_message)
    """
    delete_objects = map(lambda x: DeleteObject(x), object_names)
    delete_results = self.__minio_client.remove_objects(bucket, delete_objects)
    return [(x.name, x.message) for x in delete_results]

  def get_file(self, bucket: str, object_name: str, path: pathlib.Path) -> None:
    """
    Get object from minio and save to given path
    :param bucket: bucket name
    :param object_name: object name
    :param path: path to save
    :return: None
    """
    try:
      resp = self.__minio_client.fget_object(bucket, object_name, path)
    except Exception:  # pylint: disable=broad-except
      self.logger.exception("Got an exception while getting object")
    finally:
      resp.close()  # pylint: disable=no-member
      resp.release_conn()  # pylint: disable=no-member

  def upload_file(self, bucket: str, object_name: str, path: pathlib.Path) -> None:
    """
    Upload file to minio storage

    :param bucket: bucket name
    :param object_name: destination object name
    :param path: path of file source
    :return: None
    """
    if not os.path.isfile(path):
      mesg = f"Can't upload {path}: not present or not a file"
      self.logger.error(mesg)
      raise IOError(mesg)
    self.__minio_client.fput_object(bucket, object_name, path)

  def cleanup_storage(self, max_size: int) -> (int, typing.List[str]):
    """
    Clean up storage by removing old files until below max_size
    :param max_size: max size to use
    :return: Tuple with final size of storage used and list of buckets removed
    """

    if "THREADS" in os.environ:
      try:
        threads = int(os.environ["THREADS"])
        self.logger.debug("Using %d threads for storage cleanup", threads)
      except ValueError:
        self.logger.exception("THREADS env variable not a number, using a single thread")
        threads = 1
    else:
      threads = 1

    buckets = self.__minio_client.list_buckets()

    # must use ThreadPool since minio client is thread safe with threading only
    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
      bucket_list = executor.map(self.get_bucket_info, buckets)
    bucket_list = list(bucket_list)  # want a list and not a Generator so that we can sort
    bucket_list.sort(key=lambda x: x.last_modified)
    idx = 0
    cleaned_buckets = []
    current_size = sum(map(lambda x: x.size, bucket_list))
    while current_size > max_size and idx <= len(bucket_list):
      bucket = bucket_list[idx]
      self.logger.info("Deleting %s due to storage limits", bucket.name)
      self.delete_bucket(bucket.name)
      cleaned_buckets.append(bucket.name)
      current_size -= bucket.size
      idx += 1
    return current_size, cleaned_buckets

  def get_buckets(self) -> List[str]:
    """
    Get list of buckets in minio
    :return: list of bucket names
    """
    return [x.name for x in self.__minio_client.list_buckets()]

  def create_bucket(self, bucket: str) -> None:
    """
    Create a bucket with given id
    :return: None
    """
    self.__minio_client.make_bucket(bucket)
