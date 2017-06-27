# Copyright 2015, Yahoo Inc.
# Licensed under the terms of the Apache License, Version 2.0. See the LICENSE file associated with the project for terms.
import cPickle as pkl

# Modifications: Svebor Karaman
# Data loader for MEMEX images data format
import sys
import json
import subprocess

default_partitions_nb = 240

def check_hdfs_file(hdfs_file_path):
    proc = subprocess.Popen(["hdfs", "dfs", "-ls", hdfs_file_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = proc.communicate()
    if "Filesystem closed" in err:
        print("[check_hdfs_file: WARNING] Beware got error '{}' when checking for file: {}.".format(err, hdfs_file_path))
        sys.stdout.flush()
    print "[check_hdfs_file] out: {}, err: {}".format(out, err)
    return out, err


def hdfs_file_exist(hdfs_file_path):
    out, err = check_hdfs_file(hdfs_file_path)
    hdfs_file_exist = "_SUCCESS" in out
    return hdfs_file_exist


def load_rdd_json(sc, rdd_path):
    rdd = None
    try:
        if hdfs_file_exist(rdd_path):
            print("[load_rdd_json] trying to load rdd from {}.".format(rdd_path))
            rdd = sc.sequenceFile(rdd_path).mapValues(json.loads)
        else:
            print("[load_rdd_json] no rdd found at: {}.".format(rdd_path))
    except Exception as inst:
        print("[load_rdd_json: caught error] could not load rdd from {}. Error was {}.".format(rdd_path, inst))
    return rdd


def decode_feat(feat):
    """Decode base64 encoded feature 'feat'.
    """
    import numpy as np
    import base64
    return np.frombuffer(base64.b64decode(feat), dtype=np.float32)

def get_partitions_nb(max_samples_per_partition, nb_partitions=0, rdd_count=0):
    """ Calculate number of partitions for a RDD.
    """
    import numpy as np
    # if nb_partitions is set use that
    if nb_partitions > 0:
        partitions_nb = nb_partitions
    elif rdd_count > 0: # if options.nb_partitions is -1 (default)
        #estimate from rdd_count and options.max_samples_per_partition
        import numpy as np
        partitions_nb = int(np.ceil(float(rdd_count)/max_samples_per_partition))
    else: # fall back to default partitions nb
        partitions_nb = default_partitions_nb
    print "[get_partitions_nb: log] partitions_nb: {}".format(partitions_nb)
    return partitions_nb

def load_rdd_in(sc, data_path, sampling_ratio, seed, args, repartition):
    """ Load RDD and repartition if needed.
    """
    rdd = load_rdd_json(sc, data_path).sample(False, sampling_ratio, seed)
    if repartition and args:
        count = rdd.count()
        # we could get the count as the out_rdd_wfeat_count from hbase...
        nb_partitions_wfeat = get_partitions_nb(args.max_samples_per_partition_wfeat, args.nb_partitions, count)
        rdd_in = rdd.partitionBy(nb_partitions_wfeat)
    else:
        rdd_in = rdd
    return rdd_in

def memex_udf(sc, data_path, sampling_ratio, seed, feat_field, args, repartition):
    """
    MEMEX UDF function to load training data. 
    Loads data from a sequence file containing JSON formatted data with 
    a base64-encoded numpy arrays in field 'feat_field'.
    """
    
    # Load rdd and sample down the dataset
    rdd = load_rdd_in(sc, data_path, sampling_ratio, seed, args, repartition)

    # Load feature
    deserialize_vec = lambda s: decode_feat(s[1][feat_field])
    vecs = rdd.filter(lambda s: s[1] is not None).map(deserialize_vec)
    first_sample = vecs.first()
    print '[memex_udf: log] first sample: {}, feat shape: {}'.format(first_sample, first_sample.shape)
    return vecs


def memex_udf_wid(sc, data_path, sampling_ratio, seed, feat_field, args, repartition):
    """
    MEMEX UDF function to load training data. 
    Loads data from a sequence file containing JSON formatted data with 
    a base64-encoded numpy arrays in field 'feat_field'.
    """
    import numpy as np
    import base64
    
    # Load rdd and sample down the dataset
    rdd_in = load_rdd_in(sc, data_path, sampling_ratio, seed, args, repartition)

    # Load feature
    deserialize_vec = lambda s: (s[0], decode_feat(s[1][feat_field]))
    vecs = rdd_in.filter(lambda s: s[1] is not None).map(deserialize_vec)
    first_sample = vecs.first()
    print '[memex_udf_wid: log] first sample: {}, feat shape: {}'.format(first_sample, first_sample[1].shape)
    return vecs

