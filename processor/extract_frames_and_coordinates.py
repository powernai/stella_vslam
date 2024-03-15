import cv2
import msgpack
import numpy as np
import os
import csv
from scipy.spatial.transform import Rotation as R
import json

class FrameCoordinatesExtractor:
    def __init__(self, msg_path, video_path, dest_path):
        self.msg_path = msg_path
        self.video_path = video_path
        self.dest_path = dest_path
        print("message path", self.msg_path)
        print("video path", self.video_path)

    def convert_unix_timestamp_to_frame_index(self, unix_timestamp, initial_unix_timestamp, fps):
        # Calculate time difference in seconds
        time_difference = unix_timestamp - initial_unix_timestamp
        # Convert time difference to frame index
        frame_index = int(time_difference * fps)
        return frame_index

    def generate_frames_from_msg(self):
        with open(self.msg_path, "rb") as f:
            u = msgpack.Unpacker(f)
            msg = u.unpack()

        keyframes_data = msg.get("keyframes", {})
        timestamps = [keyframe["ts"] for keyframe in keyframes_data.values()]
        initial_timestamp = timestamps[0]

        vidcap = cv2.VideoCapture(self.video_path)
        fps = vidcap.get(cv2.CAP_PROP_FPS)

        keyfrm_points = []
        for key, value in keyframes_data.items():
            trans_cw = np.matrix(value["trans_cw"]).T
            rot_cw = R.from_quat(value["rot_cw"]).as_matrix()
            rot_wc = rot_cw.T
            trans_wc = - rot_wc * trans_cw
            keyfrm_points.append((trans_wc[0, 0], trans_wc[1, 0], trans_wc[2, 0]))

            frame_index = self.convert_unix_timestamp_to_frame_index(value['ts'], initial_timestamp, fps)
            vidcap.set(cv2.CAP_PROP_POS_FRAMES, frame_index)
            ret, frame = vidcap.read()

            if self.dest_path is None:
                self.dest_path = os.getcwd()  # Set dest_path to current working directory

            if not os.path.exists(self.dest_path):
                os.makedirs('360Images')  # Create the directory if it doesn't exist

            # Rest of the code...

            if ret:
                cv2.imwrite(os.path.join(self.dest_path, f"{key}.jpg"), frame)
            else:
                print(f"Error: Frame not found for Unix timestamp {value['ts']}")

            np.savetxt((os.path.join(self.dest_path, "keyframes.csv")), keyfrm_points, delimiter=",")
        print("Finished, all frames generated successfully.")

    def generate_final_coordinates_json(self):
        with open(self.msg_path, "rb") as f:
            u = msgpack.Unpacker(f)
            msg = u.unpack()

        key_frames = msg.get('keyframes', {})
        needed_keys = ['trans_cw', 'rot_cw']
        output_object = {"coordinates": []}

        for key in key_frames.keys():
            sample = {}
            for needed_key in needed_keys:
                if needed_key == 'trans_cw':
                    trans_cw = np.matrix(key_frames[key][needed_key]).T
                    rot_cw = R.from_quat(key_frames[key]["rot_cw"]).as_matrix()
                    rot_wc = rot_cw.T
                    trans_wc = - rot_wc * trans_cw
                    sample[needed_key] = [trans_wc[0, 0], trans_wc[1, 0], trans_wc[2, 0]]
                else:
                    sample[needed_key] = key_frames[key][needed_key]

            output_object["coordinates"].append([
                f"../360Images/{key}.jpg",
                0,
                *sample['trans_cw'],
                *sample['rot_cw']
            ])

        with open("coordinates.json", "w") as output_file:
            json.dump(output_object, output_file, indent=2)

        print(" Coordinates JSON file has been saved successfully.")

