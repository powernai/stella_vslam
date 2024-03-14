from boto3 import client
from os import environ
from dotenv import load_dotenv
import subprocess
from urllib.parse import unquote
import re
import tempfile
from concurrent import futures
from rabbitMq.publisher import Publisher
from extract_frames_and_coordinates import FrameCoordinatesExtractor
from dotenv import load_dotenv


'''
    1) Execute a python script that spawns a process to run stella_vslam_convertion
    2) Uploads the output 360 Image frames and camera Localization Output to the target path in S3 using boto3
'''

load_dotenv()


class Utils:
    @staticmethod
    def extract_s3_info(object_url: str) -> [str, str]:
        """ 
            Extracts Bucket and the object key from the S3 url resource
        """
        regex = "https://s3\.amazonaws\.com/(.*?)/(.*)"
        match = re.match(regex, object_url)
        bucket, object_key = match.group(1), unquote(match.group(2))
        return [bucket, object_key]
    
    @staticmethod
    def get_object_url(bucket: str, object_key: str) -> str:
        """
            Returns a valid S3 Object URL
        """
        return f'https://s3.amazonaws.com/{bucket}/{object_key}'

    @staticmethod  
    def extract_parent_key(object_key: str) -> str:
        """
        Returns the parent object key
        """
        return '/'.join(object_key.split('/')[:-1])


class Processor:

    def __init__(self) -> None:
        self._S3_READ_CLIENT = client(
            "s3",
            region_name=environ.get('AWS_BUCKET_REGION')
        )
        self.skip_frame = environ.get('SKIP_FRAME', 3)
        self.video_path = environ.get('VIDEO_PATH', './aist_living_lab_1/video.mp4')
        self.camera_path = environ.get('CAMERA_PATH','./camera.yaml')
        self.output_path = 'messagepack.msg'
        self.bucket = ''
        self.parent_key = ''
        self.dest_path = './360Images/'

    def localize(self):
        '''
            Run the stella-vslam localizer to extract the map information
            --auto-term end is required to complete process in the background
        '''
        subprocess.run([
                './run_video_slam', '-v', './orb_vocab.fbow', '-m', self.video_path, '-c', self.camera_path, '--frame-skip', str(self.skip_frame), '--no-sleep',
                '--map-db-out', self.output_path, '--auto-term'
            ])
            
    def extract_frames_and_coordinates(self):
        '''
            Extracts the msgpack from the output path
        '''
        service = FrameCoordinatesExtractor(self.output_path, self.video_path, self.dest_path) 
        service.generate_frames_from_msg()
        service.generate_final_coordinates_json()
        
    
    def set_bucket_and_parent_key(self):
        '''
            Sets the object and parent key for the output path
        '''
        self.bucket, object_key = Utils.extract_s3_info(self.video_path)
        self.parent_key = Utils.extract_parent_key(object_key)

    def download(self, source_url: str):
        """
            Downloads the file object from Source (s3) and creates a temporary file
        """
        bucket, object_key = Utils.extract_s3_info(
            source_url.get("s3_path"))

        temp_file = tempfile.NamedTemporaryFile(delete=False)

        self._S3_READ_CLIENT.download_fileobj(
            Bucket=bucket, Key=object_key, Fileobj=temp_file)

        return temp_file.name

    def fetch_data(self):
        '''
            Downloads the source data
        '''

        self.camera_local_path = self.download(self.camera_path)
        self.video_local_path = self.download(self.video_local_path)

    def upload(self, path):
        '''
            Uploads the processed data back to the target path
        '''
        bucket, object_key = Utils.extract_s3_info(path)
        self._S3_READ_CLIENT.upload_fileobj(open(self.output_path, 'rb'), bucket, object_key)

    def acknowledge(self):
        '''
            Acknowledges by sending a processed message back to rabbitMQ
        '''
        # TODO: This changes after the new changes in stella vslam
        message= {
            'status': 'done',
            'type': 'stella_vslam',
            'frames_url': self.extract_keyframes_output_path,
        }
        publisher = Publisher()
        publisher.publish_message(body=message)
        pass

    def process(self):
        '''
            Run all the tasks to process the source data
        '''
        self.fetch_data()
        self.localize()
        self.extract_frames_and_coordinates()
        self.set_bucket_and_parent_key()
        self.upload()
        self.acknowledge()


if __name__ == '__main__':

    processor = Processor()
    processor.localize()
    processor.extract_frames_and_coordinates()
