import sys
import msgpack
import json
import numpy as np
from scipy.spatial.transform import Rotation as R


def main(bin_fn, dest_fn):

    # Read file as binary and unpack data using MessagePack Library
    # with open(bin_fn, "rb") as f:
    #     data = msgpack.unpackb(f.read(), use_list=False, raw=False)

    with open(bin_fn, "rb") as f:
        u = msgpack.Unpacker(f)
        msg = u.unpack()

    key_frames = msg.get('keyframes', {})
    needed_keys = ['trans_cw', 'rot_cw']
    output = {}

    for key in key_frames.keys():

        output[key] = {}
        for needed_key in needed_keys:
            if needed_key == 'trans_cw':
                trans_cw = np.matrix(key_frames[key][needed_key]).T
                rot_cw = R.from_quat(key_frames[key]["rot_cw"]).as_matrix()
                rot_wc = rot_cw.T
                trans_wc = - rot_wc * trans_cw
                output[key][needed_key] = [
                    trans_wc[0, 0], trans_wc[1, 0], trans_wc[2, 0]]
            elif needed_key == 'rot_cw':
                    print(key_frames[key]["rot_cw"])
                    rot_cw = R.from_quat(key_frames[key]["rot_cw"]).as_matrix()
                    rot_wc = rot_cw.T
                    output[key][needed_key] = [
                        rot_wc[0, 0], rot_wc[1, 0], rot_wc[2, 0]]
            else:
                output[key][needed_key] = key_frames[key][needed_key]

    # Write point coordinates into file, one point for one line
    with open(dest_fn, "w") as json_file:
        json.dump(output, json_file, indent=2)

    print("Finished")


if __name__ == "__main__":
    argv = sys.argv

    if len(argv) < 3:
        print("Read al content in the map file and dump into a json file")
        print("Usage :")
        print("python msg_to_json.py [msg file] [json destination]")

    else:
        bin_fn = argv[1]
        dest_fn = argv[2]
        main(bin_fn, dest_fn)
