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
        # getting the environment variables
        self.skip_frame = environ.get('SKIP_FRAME', 3)
        self.video_path = environ.get('VIDEO_PATH')
        self.camera_path = environ.get('CAMERA_PATH')
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
        date_folder = datetime.datetime.now().strftime("%Y-%m-%d")
        folder_path = os.path.join(self.date, '360Images' )
        try:
            self._S3_READ_CLIENT.head_object(Bucket=self.bucket, Key=self.parent_key + f"/{folder_path}")
            print("Folder already exists in S3, key: ", self.parent_key + f"/{folder_path}")
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '404':
                self._S3_READ_CLIENT.put_object(Bucket=self.bucket, Key=self.parent_key + f"/{folder_path}")
                print("Created folder in S3, key: ", self.parent_key + f"/{folder_path}")
       
       
        #get all files to upload
        files = os.listdir(self.dest_path)
        files_to_upload = [os.path.join(self.dest_path, file) for file in files if file.endswith('.jpg')]
        
        batch_size = self.batch_size
        num_threads = self.num_threads
        
        # Use ThreadPoolExecutor to upload files concurrently
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
            # Divide files into batches
            batches = [files_to_upload[i:i+batch_size] for i in range(0, len(files_to_upload), batch_size)]
            futures = []
            
            # Upload each batch of files concurrently
            for batch in batches:
                # Submit upload task for the batch
                futures.extend([executor.submit(self.upload_batch, self.bucket, file_name, self.parent_key + f"/{folder_path}/{file_name.split('/')[-1]}") for file_name in batch])
                
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
            self._S3_READ_CLIENT.upload_fileobj(Fileobj=fp, Bucket=self.bucket, Key=self.parent_key + f"/{self.date}/{self.coordinates_file}")
        print(f"Uploaded coordinates JSON file " )
        
        self.images_url = Utils.get_object_url(self.bucket, self.parent_key + f"/{folder_path}")
        self.coordinates_url = Utils.get_object_url(self.bucket, self.parent_key + f"/{self.date}/{self.coordinates_file}")
        

    def acknowledge(self):
        '''
            Acknowledges by sending a processed message back to rabbitMQ
        '''
        # TODO: This changes after the new changes in stella vslam
        message= {
            'status': 'done',
            'type': 'stella_vslam',
            'images_url': self.images_url,
            'coordinates_url': self.coordinates_url,
            'date': self.date,
            'project_id': environ.get('PROJECT_ID'),
            'video_version_id': environ.get('VIDEO_VERSION_ID'),
            'camera_version_id': environ.get('CAMERA_VERSION_ID'), 
            'msg_id': environ.get('MSG_ID'),
            'image_count': self.num_imgs if self.num_imgs else 0 
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

    all_videos_urls = os.environ.get('ALL_VIDEO_PATH',[{'url':'https://s3.amazonaws.com/ai.powern.website.assets/cpms/FWH/360Slam/video.mp4','version_id':"version-id-hardcoded"}])
    all_camera_urls = os.environ.get('ALL_CAMERA_PATH',[{'url':'https://s3.amazonaws.com/ai.powern.website.assets/cpms/FWH/360Slam/equirectangular.yaml'}])
    all_dates = os.environ.get('ALL_DATES',[datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")])
    project_id = os.environ.get('PROJECT_ID', '516165165165')
    msg_id = os.environ.get('MSG_ID', '44564563453543453')
    environ['PROJECT_ID'] = project_id
    environ['MSG_ID'] = msg_id
    
    count = len(all_dates)
    for i in range(count):
        environ['VIDEO_PATH'] = all_videos_urls[i]['url']
        environ['CAMERA_PATH'] = all_camera_urls[i]['url']
        environ['VIDEO_VERSION_ID']= all_videos_urls[i].get('version_id',None)
        environ['CAMERA_VERSION_ID']= all_camera_urls[i].get('version_id',None)
        formatted_date = datetime.datetime.strptime(all_dates[i], "%Y-%m-%dT%H:%M:%S.%fZ").strftime("%Y-%m-%d")  
        environ['DATE'] = formatted_date
        
        processor = Processor()
        processor.process()


