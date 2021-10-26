import datetime
import unittest
from collections import namedtuple

import pytest
from unittest.mock import ANY, MagicMock, patch

import servicex_storage.minio_storage_manager

ObjectInfo = namedtuple('ObjectInfo', ['size', 'last_modified'])
minio_fake_objects = {
  "bucket1": {
    "object1": ObjectInfo(size=10,
                          last_modified=datetime.datetime(year=2021, month=10, day=1, hour=10, minute=10, second=10)),
    "object2": ObjectInfo(size=20,
                          last_modified=datetime.datetime(year=2021, month=10, day=1, hour=10, minute=11, second=10)),
    "object3": ObjectInfo(size=30,
                          last_modified=datetime.datetime(year=2021, month=10, day=1, hour=10, minute=12, second=10)),
  },
  "bucket2": {
    "object4": ObjectInfo(size=100,
                          last_modified=datetime.datetime(year=2020, month=10, day=1, hour=10, minute=10, second=10)),
    "object5": ObjectInfo(size=200,
                          last_modified=datetime.datetime(year=2020, month=10, day=1, hour=10, minute=11, second=10)),
    "object6": ObjectInfo(size=300,
                          last_modified=datetime.datetime(year=2020, month=10, day=1, hour=10, minute=12, second=10)),
  }
}


class MyTestCase(unittest.TestCase):
  @patch('minio.Minio')
  def test_minio_get_bucket_info(self, mock_class):
    """
    Test minio's get bucket info
    :return: None
    """

    mock_class().list_objects.return_value = list(minio_fake_objects["bucket1"].keys())
    mock_class().stat_object.side_effect = list(minio_fake_objects["bucket1"].values())
    return_value = servicex_storage.minio_storage_manager.BucketInfo(name="bucket1",
                                                                     size=60,
                                                                     last_modified=datetime.datetime(
                                                                       year=2021, month=10,
                                                                       day=1, hour=10,
                                                                       minute=10, second=10))
    test_obj = servicex_storage.minio_storage_manager.MinioStore(minio_url="abc",
                                                                 access_key="abc",
                                                                 secret_key="abc")
    bucket_info = test_obj.get_bucket_info("bucket1")
    self.assertEqual(bucket_info, return_value)

  @patch('minio.Minio')
  def test_minio_get_storage_used(self, mock_class):
    """
    Test minio's get bucket info
    :return: None
    """
    mock_class().list_buckets.return_value = list(minio_fake_objects.keys())
    mock_class().list_objects.side_effect = [list(minio_fake_objects["bucket1"].keys()),
                                             list(minio_fake_objects["bucket2"].keys())]
    mock_class().stat_object.side_effect = list(minio_fake_objects["bucket1"].values()) + \
                                           list(minio_fake_objects["bucket2"].values())

    test_obj = servicex_storage.minio_storage_manager.MinioStore(minio_url="abc",
                                                                 access_key="abc",
                                                                 secret_key="abc")

    bucket_size = test_obj.get_storage_used()
    self.assertEqual(bucket_size, 660)

  @patch('minio.Minio')
  def test_minio_cleanup_storage(self, mock_class):
    """
    Test minio's get bucket info
    :return: None
    """
    mock_class().list_buckets.return_value = list(minio_fake_objects.keys())
    mock_class().list_objects.side_effect = [list(minio_fake_objects["bucket1"].keys()),
                                             list(minio_fake_objects["bucket2"].keys()),
                                             list(minio_fake_objects["bucket2"].keys())]
    mock_class().stat_object.side_effect = list(minio_fake_objects["bucket1"].values()) + \
                                           list(minio_fake_objects["bucket2"].values())

    test_obj = servicex_storage.minio_storage_manager.MinioStore(minio_url="abc",
                                                                 access_key="abc",
                                                                 secret_key="abc")

    final_size = test_obj.cleanup_storage(70)
    self.assertEqual(final_size, 60)
    mock_class().remove_objects.assert_called_with("bucket2", ["object4", "object5", "object6"])


if __name__ == '__main__':
  unittest.main()
