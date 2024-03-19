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
import boto3
import botocore
import concurrent.futures
import urllib
import ast
from constants import PROCESSING_TYPE
import shutil


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

    def __init__(self, video_path, camera_path, video_version_id, camera_version_id) -> None:
        self._S3_READ_CLIENT = client(
            "s3",
            region_name=environ.get('AWS_BUCKET_REGION'),
            aws_access_key_id=environ.get('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=environ.get('AWS_SECRET_ACCESS_KEY')
        )
        self.video_path = video_path
        self.camera_path = camera_path
        self.video_version_id = video_version_id
        self.camera_version_id = camera_version_id
        
        # getting the environment variables
        self.skip_frame = environ.get('SKIP_FRAME', 3)
        self.date = environ.get('DATE')
        self.EBS_PATH = environ.get('EBS_PATH', None)
        self.batch_size = environ.get('BATCH_SIZE', 10)
        self.num_threads = environ.get('NUM_THREADS', 2)
        
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
        Downloads the file object from Source (S3) and creates a temporary file or downloads to EBS volume if provided.
        """
        if isinstance(source_url, dict):
            bucket, object_key = Utils.extract_s3_info(source_url.get("s3_path"))
        else:
            bucket, object_key = Utils.extract_s3_info(source_url)

        ebs_volume_path = self.EBS_PATH
        if ebs_volume_path is not None:
            file_name = urllib.parse.unquote_plus(os.path.basename(object_key))
            # Download the file to the specified EBS volume path with the original file name
            ebs_file_path = os.path.join(ebs_volume_path, file_name)
            self._S3_READ_CLIENT.download_file(
                Bucket=bucket, Key=object_key, Filename=ebs_file_path)
            return ebs_file_path
        else:
            # Create a temporary file in tmpfs
            temp_file = tempfile.NamedTemporaryFile(delete=False)
            try:
                # Download to the temporary file
                self._S3_READ_CLIENT.download_fileobj(
                    Bucket=bucket, Key=object_key, Fileobj=temp_file)
                return temp_file.name
            except Exception as e:
                # Clean up the temporary file if download fails
                temp_file.close()
                os.unlink(temp_file.name)
                raise e

    def fetch_data(self):
        '''
            Downloads the source data
        '''
        self.camera_local_path = self.download(self.camera_path)
        self.video_local_path = self.download(self.video_path)
        print("Downloaded the source data")
        
    def upload_batch(self,bucket_name, file_name, key):
        self._S3_READ_CLIENT.upload_file(file_name, bucket_name, key)
        
    def upload(self):
        '''
            Uploads the processed data back to the target path
        '''
        self.folder_path = os.path.join(self.date, '360Images/' )
        self.coordinates_path = os.path.join(self.date, 'coordinates/')
        try:
            self._S3_READ_CLIENT.head_object(Bucket=self.bucket, Key=self.parent_key + f"/{self.folder_path}")
            print("Folder already exists in S3, key: ", self.parent_key + f"/{self.folder_path}")
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '404':
                self._S3_READ_CLIENT.put_object(Bucket=self.bucket, Key=self.parent_key + f"/{self.folder_path}")
                self._S3_READ_CLIENT.put_object(Bucket=self.bucket, Key=self.parent_key + f"/{self.coordinates_path}")
                print("Created folder in S3, key: ", self.parent_key + f"/{self.folder_path}")
       
       
        #get all files to upload
        files = os.listdir(self.dest_path)
        files_to_upload = [os.path.join(self.dest_path, file) for file in files if file.endswith('.jpg')]
        
        batch_size = int(self.batch_size)
        num_threads = int(self.num_threads)
        
        # Use ThreadPoolExecutor to upload files concurrently
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
            # Divide files into batches
            batches = [files_to_upload[i:i+batch_size] for i in range(0, len(files_to_upload), batch_size)]
            futures = []
            
            # Upload each batch of files concurrently
            for batch in batches:
                # Submit upload task for the batch
                futures.extend([executor.submit(self.upload_batch, self.bucket, file_name, self.parent_key + f"/{self.folder_path}{file_name.split('/')[-1]}") for file_name in batch])
                
            # Wait for all futures to complete
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()  
                except Exception as e:
                    print(f'Error uploading file: {e}')
            self.num_imgs = len(files_to_upload)
            print(f"Uploaded {len(files_to_upload)} files to S3")

        #upload coordinates.json file in s3
        file_obj_path = os.path.basename(self.coordinates_file)
        with open(file_obj_path, 'rb') as fp:
            self._S3_READ_CLIENT.upload_fileobj(Fileobj=fp, Bucket=self.bucket, Key=self.parent_key + f"/{self.coordinates_path}{self.coordinates_file}")
        print(f"Uploaded coordinates.json file " )
        
        self.images_url = Utils.get_object_url(self.bucket, self.parent_key + f"/{self.folder_path}")
        self.coordinates_url = Utils.get_object_url(self.bucket, self.parent_key + f"/{self.coordinates_path}{self.coordinates_file}")
        

    def acknowledge(self):
        '''
            Acknowledges by sending a processed message back to rabbitMQ
        '''
        update_msg = False
        processed_count = environ.get('processing_count')
        total_count = environ.get('TOTAL_COUNT')
        if int(processed_count) == int(total_count):
            update_msg = True
        # TODO: This changes after the new changes in stella vslam
        message= {
            'status': 'done',
            'type': PROCESSING_TYPE,
            'images_url': self.images_url,
            'coordinates_url': self.coordinates_url,
            'date': self.date,
            'project_id': environ.get('PROJECT_ID'),
            'video_version_id': self.video_version_id,
            'camera_version_id': self.camera_version_id, 
            'msg_id': environ.get('MSG_ID'),
            'update_msg': update_msg,
            'image_count': self.num_imgs if self.num_imgs else 0 
          }
            
        publisher = Publisher()
        publisher.publish_message(body=message)
        print("Acknowledged: message sent with body: ", message)
        
    def cleanup(self):
        # Clean up temporary files
        print("cleanup started")
        file_paths = [self.video_local_path, self.camera_local_path, self.output_path, self.coordinates_file, self.dest_path]
        for path in file_paths:
            if os.path.isfile(path):
                os.remove(path)
            elif os.path.isdir(path):
                shutil.rmtree(path)
        print("Deleted temporary files")

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
        self.cleanup()


if __name__ == '__main__':

    all_videos_urls = ast.literal_eval(os.environ.get('ALL_VIDEO_PATH', str([{'url':'https://s3.amazonaws.com/ai.powern.website.assets/cpms/FWH/360Slam/video.mp4','version_id':"1version-id-hardcoded"},{'url':'https://s3.amazonaws.com/ai.powern.website.assets/cpms/FWH/360Slam/video.mp4','version_id':"2version-id-hardcoded"}])))
    all_camera_urls = ast.literal_eval(os.environ.get('ALL_CAMERA_PATH',str([{'url':'https://s3.amazonaws.com/ai.powern.website.assets/cpms/FWH/360Slam/equirectangular.yaml','version_id':"1version-id-hardcoded"},{'url':'https://s3.amazonaws.com/ai.powern.website.assets/cpms/FWH/360Slam/equirectangular.yaml','version_id':"2version-id-hardcoded"}])))
    all_dates = ast.literal_eval(os.environ.get('ALL_DATES',str([datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ"),"2022-01-01T12:34:56.789012Z"])))
    project_id = os.environ.get('PROJECT_ID', '63aa5983-8905-4506-97fd-53ed509cabf0')
    msg_id = os.environ.get('MSG_ID', '63aa5983-8905-4506-97fd-53ed509cabf0')
    environ['PROJECT_ID'] = project_id
    environ['MSG_ID'] = msg_id
    
    total_count = len(all_dates)
    environ['TOTAL_COUNT'] = str(total_count)
    
    for i in range(total_count):
        print("all got",all_videos_urls)
        print(f"Processing {i+1} of {total_count}", all_videos_urls[i].get('url'))
        video_path = all_videos_urls[i]['url']
        camera_path = all_camera_urls[i]['url']
        video_version_id= all_videos_urls[i].get('version_id',None)
        camera_version_id= all_camera_urls[i].get('version_id',None)
        
        environ['DATE'] = datetime.datetime.strptime(all_dates[i], "%Y-%m-%dT%H:%M:%S.%fZ").strftime("%Y-%m-%d") 
        environ['processing_count'] = str(i+1)
                
        processor = Processor(video_path, camera_path, video_version_id, camera_version_id)
        processor.process()


