import datetime
import os
import pathlib
import concurrent.futures
import sys
from collections import namedtuple

import minio

from servicex_storage import object_storage_manager

BucketInfo = namedtuple('BucketInfo', ['name', 'size', 'last_modified'])


class MinioStore(object_storage_manager.ObjectStore):
  """
  Class to handle operations for minio storage
  """
  def __init__(self, minio_url: str, access_key: str, secret_key: str):
    super().__init__()
    self.minio_url = minio_url
    self.access_key = access_key
    self.secret_key = secret_key
    # client is thread safe using Threading, not so much with multiprocessing
    self.__minio_client = minio.Minio(self.minio_url, access_key=self.access_key, secret_key=self.secret_key)

  def get_bucket_info(self, bucket: str) -> BucketInfo:
    """
    Given a bucket, get the size and last modified date

    :param bucket: bucket name
    :return: None
    """

    objects = self.__minio_client.list_objects(bucket)
    size = 0
    last_modified = datetime.datetime.now()
    print(bucket)
    for obj in objects:
      result = self.__minio_client.stat_object(bucket, obj)
      size += result.size
      if result.last_modified < last_modified:
        last_modified = result.last_modified
    return BucketInfo(name=bucket, size=size, last_modified=last_modified)

  def delete_bucket(self, bucket: str) -> None:
    """
    Delete a given bucket and contents from minio

    :param bucket: bucket name
    :return:  None
    """
    if not self.__minio_client.bucket_exists(bucket):
      return
    objects = self.__minio_client.list_objects(bucket)
    self.__minio_client.remove_objects(bucket, objects)
    self.__minio_client.remove_bucket(bucket)

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
      except ValueError:
        threads = 1
    else:
      threads = 1

    sizes = []
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
    finally:
      resp.close()
      resp.release_conn()

  def upload_file(self, bucket: str, object_name: str, path: pathlib.Path) -> None:
    """
    Upload file to minio storage

    :param bucket: bucket name
    :param object_name: destination objecct name
    :param path: path of file source
    :return: None
    """
    if not os.path.isfile(path):
      raise IOError(f"Can't upload {path}: not present or not a file")
    self.__minio_client.fput_object(bucket, object_name, path)

  def cleanup_storage(self, max_size: int) -> int:
    """
    Clean up storage by removing old files until below max_size
    :param max_size: max size to use
    :return: Final size of storage used
    """

    if "THREADS" in os.environ:
      try:
        threads = int(os.environ["THREADS"])
      except ValueError:
        threads = 1
    else:
      threads = 1

    buckets = self.__minio_client.list_buckets()
    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
      bucket_list = executor.map(self.get_bucket_info, buckets)
    bucket_list = list(bucket_list)  # want a list and not an Iterator
    bucket_list.sort(key=lambda x: x.last_modified)
    idx = 0
    current_size = sum(map(lambda x: x.size, bucket_list))
    while current_size > max_size and idx <= len(bucket_list):
      bucket = bucket_list[idx]
      self.delete_bucket(bucket.name)
      current_size -= bucket.size
      idx += 1
    return current_size
