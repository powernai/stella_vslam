import cv2
import msgpack
import numpy as np
import os
import csv
from scipy.spatial.transform import Rotation as R


# msg_path = '/home/powern/Downloads/map.msg'
# msg_path = '/home/powern/360_data/AIST/map.msg'
msg_path = '/home/powern/Downloads/1st_FWH_map.msg'
video_path = '/home/powern/360_data/fwh_video.mp4'
# video_path = '/home/powern/Downloads/aist_living_lab_1/video.mp4'
dest_path = '/home/powern/codebase/poc/stella_vslam_parser/output3'

with open(msg_path, "rb") as f:
    u = msgpack.Unpacker(f)
    msg = u.unpack()

keyframes_data = msg["keyframes"]
# landmarks_data = msg["landmarks"]
timestamps = []

for keyframe in keyframes_data.values():
    timestamps.append(keyframe["ts"])

# print(timestamps[0])

# for initial timestamp
initial_timestamp = timestamps[0]


# For conversion of timestamp to frame index
def convert_unix_timestamp_to_frame_index(unix_timestamp, initial_unix_timestamp, fps):
    # Calculate time difference in seconds
    time_difference = unix_timestamp - initial_unix_timestamp

    # Convert time difference to frame index
    frame_index = int(time_difference * fps)

    return frame_index


vidcap = cv2.VideoCapture(video_path)
fps = vidcap.get(cv2.CAP_PROP_FPS)


keyfrm_points = []

for key, value in keyframes_data.items():

    # key has the keyframe number

    # get conversion from camera to world
    trans_cw = np.matrix(value["trans_cw"]).T
    rot_cw = R.from_quat(value["rot_cw"]).as_matrix()

    # compute conversion from world to camera
    rot_wc = rot_cw.T
    trans_wc = - rot_wc * trans_cw
    # print((trans_wc[0, 0], trans_wc[1, 0], trans_wc[2, 0]))

    keyfrm_points.append((trans_wc[0, 0], trans_wc[1, 0], trans_wc[2, 0]))

    with open(os.path.join(dest_path, ("keyframe_"+str(key)+".csv")), "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow((trans_wc[0, 0], trans_wc[1, 0], trans_wc[2, 0]))

    frame_index = convert_unix_timestamp_to_frame_index(
        value['ts'], initial_timestamp, fps)

    vidcap.set(cv2.CAP_PROP_POS_FRAMES, frame_index)
    ret, frame = vidcap.read()

    if ret:
        # print(f"Found frame at Unix timestamp {keyfrm['ts']}, Frame Index: {frame_index}")
        cv2.imwrite(os.path.join(dest_path, (str(key)+".jpg")), frame)
        # Use the frame as needed
    else:
        print(f"Error: Frame not found for Unix timestamp {value['ts']}")


np.savetxt((os.path.join(dest_path, "keyframes.csv")),
           keyfrm_points, delimiter=",")
print("Finished")


# Landmark points

# landmark_points = []

# for lm in landmarks_data.values():
#     landmark_points.append(lm["pos_w"])
#     if not os.path.join(path, ("landmark_"+str(lm['1st_keyfrm'])+".csv")):
#         with open(os.path.join(path, ("landmark_"+str(lm['1st_keyfrm'])+".csv")), "w", newline="") as csvfile:
#             writer = csv.writer(csvfile)
#             writer.writerow(lm["pos_w"])

#     else:
#         with open(os.path.join(path, ("landmark_"+str(lm['1st_keyfrm'])+".csv")), "a", newline="") as csvfile:
#             writer = csv.writer(csvfile)
#             writer.writerow(lm["pos_w"])

# np.savetxt((os.path.join(path, "landmarks.csv")),
#            landmark_points, delimiter=",")

print("Completed")
