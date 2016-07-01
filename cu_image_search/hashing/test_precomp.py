import os
import struct
import numpy as np

if __name__=="__main__":
    query_precomp_fn = "query_precomp"
    features_fn = "feats_precomp.dat"
    hashcodes_fn = "hashcodes_precomp.dat"
    base_updatepath = "/home/ubuntu/memex/update/"
    feature_num = 4096
    bits_num = 256
    list_feats_id = [1, 1000, 10000, 100000, 1000000, 10000000]
    with open(query_precomp_fn,"wb") as f_pre:
        for feat_id in list_feats_id:
            f_pre.write(struct.pack('i',feat_id))
    # query features
    command = "./get_precomp_feats {} {} {}".format(query_precomp_fn,features_fn,base_updatepath)
    print command
    os.system(command)
    feats = []
    with open(features_fn,"rb") as f_prefeats:
        for i in range(len(list_feats_id)):
            feats.append(np.frombuffer(f_prefeats.read(feature_num*4),dtype=np.float32))
    for i in range(len(list_feats_id)):
        print feats[i]
        print np.max(feats[i])
        print feats[i].shape
    # query hashcodes
    command = "./get_precomp_hashcodes {} {} {}".format(query_precomp_fn,hashcodes_fn,base_updatepath)
    print command
    os.system(command)
    hashcodes = []
    with open(hashcodes_fn,"rb") as f_prehash:
        for i in range(len(list_feats_id)):
            hashcodes.append(np.frombuffer(f_prehash.read(bits_num/8),dtype=np.uint8))
    for i in range(len(list_feats_id)):
        print hashcodes[i]
        tmp = np.unpackbits(hashcodes[i])
        print tmp.shape, tmp
        print np.max(hashcodes[i])
        print hashcodes[i].shape
