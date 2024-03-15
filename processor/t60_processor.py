from boto3 import client
from os import environ
from dotenv import load_dotenv
import subprocess
from urllib.parse import unquote
import re
import os
import tempfile
from concurrent import futures
from rabbitMq.publisher import Publisher
from extract_frames_and_coordinates import FrameCoordinatesExtractor
from dotenv import load_dotenv
import datetime


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
            region_name=environ.get('AWS_BUCKET_REGION'),
            aws_access_key_id=environ.get('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=environ.get('AWS_SECRET_ACCESS_KEY')
        )
        self.skip_frame = environ.get('SKIP_FRAME', 3)
        self.video_path = environ.get('VIDEO_PATH', 'https://s3.amazonaws.com/ai.powern.website.assets/cpms/FWH/360Slam/video.mp4')
        self.camera_path = environ.get('CAMERA_PATH','https://s3.amazonaws.com/ai.powern.website.assets/cpms/FWH/360Slam/equirectangular.yaml')
        self.output_path = 'messagepack.msg'
        self.bucket = ''
        self.parent_key = ''
        self.dest_path = './360Images/'
        self.coordinates_file = 'coordinates.json'

    def localize(self):
        '''
            Run the stella-vslam localizer to extract the map information
            --auto-term end is required to complete process in the background
        '''
        subprocess.run([
                './run_video_slam', '-v', './orb_vocab.fbow', '-m', self.video_local_path, '-c', self.camera_local_path, '--frame-skip', str(self.skip_frame), '--no-sleep',
                '--map-db-out', self.output_path, '--auto-term'
            ])
    def extract_frames_and_coordinates(self):
        '''
            Extracts the msgpack from the output path
        '''
        service = FrameCoordinatesExtractor(self.output_path, self.video_local_path, self.dest_path) 
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
        if isinstance(source_url, dict):
            bucket, object_key = Utils.extract_s3_info(source_url.get("s3_path"))
        else:
            # Handle the case when source_url is a string
            bucket, object_key = Utils.extract_s3_info(source_url)
        temp_file = tempfile.NamedTemporaryFile(delete=False)
        self._S3_READ_CLIENT.download_fileobj(
            Bucket=bucket, Key=object_key, Fileobj=temp_file)
        return temp_file.name


    def fetch_data(self):
        '''
            Downloads the source data
        '''
        self.camera_local_path = self.download(self.camera_path)
        self.video_local_path = self.download(self.video_path)
        print("Downloaded the source data")
        
        
    def upload(self):
        '''
            Uploads the processed data back to the target path
        '''
        date_folder = datetime.datetime.now().strftime("%Y-%m-%d")
        folder_path = os.path.join(date_folder, '360Images' )
        self._S3_READ_CLIENT.put_object(Bucket=self.bucket, Key=self.parent_key + f"/{folder_path}")
        print("created folder in s3, key: ", self.parent_key + f"/{folder_path}")

        for root, dirs, files in os.walk(self.dest_path):
            for file in files:
                if file.endswith(".jpg"):
                    local_file_path = os.path.join(root, file)
                    with open(local_file_path, 'rb') as fp:
                        self._S3_READ_CLIENT.upload_fileobj(
                            Fileobj=fp, Bucket=self.bucket, Key=self.parent_key + f"/{folder_path}/{file}")
            print("uploaded all images in :", self.parent_key + f"/{folder_path}")

        #upload coordinates.json file in s3
        file_obj_path = os.path.basename(self.coordinates_file)
        with open(file_obj_path, 'rb') as fp:
            self._S3_READ_CLIENT.upload_fileobj(Fileobj=fp, Bucket=self.bucket, Key=self.parent_key + f"/{date_folder}/{self.coordinates_file}")
        print(f"Uploaded coordinates JSON file " )
        
        self.images_url = Utils.get_object_url(self.bucket, self.parent_key + f"/{folder_path}")
        self.coordinates_url = Utils.get_object_url(self.bucket, self.parent_key + f"/{date_folder}/{self.coordinates_file}")
        

    def acknowledge(self):
        '''
            Acknowledges by sending a processed message back to rabbitMQ
        '''
        # TODO: This changes after the new changes in stella vslam
        message= {
            'status': 'done',
            'type': 'stella_vslam',
            'images_url': self.images_url,
            'coordinates_url': self.coordinates_url
        }
        publisher = Publisher()
        publisher.publish_message(body=message)
        print("Acknowledged: message sent with body: ", message)
        
    def delete_temp_dir(self):
        # Clean up temporary files
        temp_dir = tempfile.gettempdir()
        temp_files = os.listdir(temp_dir)
        for file in temp_files:
            file_path = os.path.join(temp_dir, file)
            if os.path.isfile(file_path):
                os.remove(file_path)
                print(f"Deleted {file_path}")

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
        self.delete_temp_dir()


if __name__ == '__main__':

    processor = Processor()
    processor.process()

