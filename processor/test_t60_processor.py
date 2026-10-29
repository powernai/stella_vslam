"""
    Tests for the S3 output paths of the t60 processor.
    Run from the processor directory: python3 -m unittest test_t60_processor
"""
import importlib.util
import os
import sys
import tempfile
import types
import unittest
from unittest import mock


class _ClientError(Exception):
    def __init__(self, error_response, operation_name):
        super().__init__(operation_name)
        self.response = error_response


def _stub_missing_modules():
    """
        Stub the heavy runtime dependencies (boto3, cv2, pika...) when they are not
        installed, so the path logic can be tested with the standard library only
    """
    def stub(name, **attrs):
        if name in sys.modules:
            return
        try:
            if importlib.util.find_spec(name) is not None:
                return
        except (ImportError, ValueError):
            pass
        module = types.ModuleType(name)
        module.__dict__.update(attrs)
        sys.modules[name] = module

    stub('boto3', client=mock.MagicMock())
    stub('botocore')
    stub('botocore.exceptions', ClientError=_ClientError)
    sys.modules['botocore'].exceptions = sys.modules['botocore.exceptions']
    stub('dotenv', load_dotenv=lambda *args, **kwargs: None)
    stub('pika')
    stub('pika.exchange_type', ExchangeType=mock.MagicMock())
    # Not under test, and it needs cv2 / scipy at import time
    sys.modules['extract_frames_and_coordinates'] = types.SimpleNamespace(
        FrameCoordinatesExtractor=mock.MagicMock())


_stub_missing_modules()
import t60_processor  # noqa: E402


class FakeS3Client:
    """
        Records every key written, and reports every key as missing on head_object
    """

    def __init__(self):
        self.keys = set()

    def head_object(self, Bucket, Key):
        raise t60_processor.botocore.exceptions.ClientError(
            {'Error': {'Code': '404'}}, 'HeadObject')

    def put_object(self, Bucket, Key, **kwargs):
        self.keys.add(Key)

    def upload_file(self, file_name, bucket, key):
        self.keys.add(key)

    def upload_fileobj(self, Fileobj, Bucket, Key):
        self.keys.add(Key)


VIDEO_URL = 'https://s3.amazonaws.com/bucket/project/streetview/video_v{}.mp4'


class OutputFolderTest(unittest.TestCase):

    def test_folder_is_date_and_video_version(self):
        self.assertEqual(
            t60_processor.Utils.get_output_folder('2026-10-29', 'abc-123'),
            '2026-10-29_abc-123')

    def test_missing_video_version_is_rejected(self):
        for version_id in (None, ''):
            with self.assertRaises(ValueError):
                t60_processor.Utils.get_output_folder('2026-10-29', version_id)


class UploadPathsTest(unittest.TestCase):

    def setUp(self):
        self.cwd = os.getcwd()
        self.workdir = tempfile.mkdtemp()
        os.chdir(self.workdir)
        # upload() reads coordinates.json from the working directory
        with open('coordinates.json', 'w') as fp:
            fp.write('{"coordinates": []}')

    def tearDown(self):
        os.chdir(self.cwd)

    def upload_version(self, version_no, video_version_id):
        """
            Runs upload() for one version with two extracted frames, returns the processor and keys
        """
        s3 = FakeS3Client()
        with mock.patch.object(t60_processor, 'client', return_value=s3), \
                mock.patch.dict(os.environ, {'DATE': '2026-10-29'}):
            processor = t60_processor.Processor(
                VIDEO_URL.format(version_no), 'camera_url', video_version_id, 'camera-version')
        processor.dest_path = tempfile.mkdtemp(dir=self.workdir)
        for frame in ('0.jpg', '1.jpg'):
            open(os.path.join(processor.dest_path, frame), 'w').close()
        processor.set_bucket_and_parent_key()
        processor.upload()
        return processor, s3.keys

    def test_versions_uploaded_on_same_date_do_not_share_keys(self):
        first, first_keys = self.upload_version(1, 'version-1')
        second, second_keys = self.upload_version(2, 'version-2')

        self.assertEqual(first_keys, {
            'project/streetview/2026-10-29_version-1/360Images/',
            'project/streetview/2026-10-29_version-1/coordinates/',
            'project/streetview/2026-10-29_version-1/360Images/0.jpg',
            'project/streetview/2026-10-29_version-1/360Images/1.jpg',
            'project/streetview/2026-10-29_version-1/coordinates/coordinates.json',
        })
        self.assertFalse(first_keys & second_keys)
        self.assertNotEqual(first.images_url, second.images_url)
        self.assertNotEqual(first.coordinates_url, second.coordinates_url)
        self.assertEqual(
            second.coordinates_url,
            'https://s3.amazonaws.com/bucket/project/streetview/'
            '2026-10-29_version-2/coordinates/coordinates.json')


if __name__ == '__main__':
    unittest.main()
