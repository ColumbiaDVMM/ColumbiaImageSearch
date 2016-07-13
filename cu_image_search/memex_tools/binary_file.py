def read_binary_file(X_fn,str_precomp,list_feats_id,read_dim,read_type):
    import numpy as np
    X = []
    ok_ids = []
    with open(X_fn,"rb") as f_preout:
        for i in range(len(list_feats_id)):
            try:
                X.append(np.frombuffer(f_preout.read(read_dim),dtype=read_type))
                ok_ids.append(i)
            except Exception as inst:
                print "[read_binary_file: error] Could not read requested {} with id {}. {}".format(str_precomp,list_feats_id[i],inst)
    return X,ok_ids

def write_binary_file(X_fn,list_feats):
    import struct
    with  open(X_fn,"wb") as f_preout:
        for i in range(len(list_feats)):
            try:
                f_preout.write(struct.pack('i',list_feats[i]))
            except Exception as inst:
                print "[write_binary_file: error] Could write element {} to file {}. {}".format(i,X_fn,inst)