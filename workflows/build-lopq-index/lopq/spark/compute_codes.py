# Copyright 2015, Yahoo Inc.
# Licensed under the terms of the Apache License, Version 2.0. See the LICENSE file associated with the project for terms.
from pyspark.context import SparkContext

# Modifications by Svebor Karaman

import os
import json
import base64
import subprocess
import numpy as np
import cPickle as pkl
from tempfile import NamedTemporaryFile, mkdtemp

from lopq.model import LOPQModel


def default_data_loading(sc, data_path, sampling_ratio, seed):
    """
    This function loads data from a text file, sampling it by the provided
    ratio and random seed, and interprets each line as a tab-separated (id, data) pair
    where 'data' is assumed to be a base64-encoded pickled numpy array.
    The data is returned as an RDD of (id, numpy array) tuples.
    """
    # Compute the number of cores in our cluster - used below to heuristically set the number of partitions
    total_cores = int(sc._conf.get('spark.executor.instances')) * int(sc._conf.get('spark.executor.cores'))

    # Load and sample down the dataset
    d = sc.textFile(data_path, total_cores * 3).sample(False, sampling_ratio, seed)

    # The data is (id, vector) tab-delimited pairs where each vector is
    # a base64-encoded pickled numpy array
    d = d.map(lambda x: x.split('\t')).map(lambda x: (x[0], pkl.loads(base64.decodestring(x[1]))))

    return d


def copy_from_hdfs(hdfs_path):
    from tempfile import mkdtemp
    import subprocess
    tmp_dir = mkdtemp()
    subprocess.call(['hadoop', 'fs', '-copyToLocal', hdfs_path, tmp_dir])
    return os.path.join(tmp_dir, hdfs_path.split('/')[-1])


def apply_PCA(x, mu, P):
    """
    Example of applying PCA.
    """
    return np.dot(x - mu, P)


def main(sc, args, data_load_fn=default_data_loading):

    # Load model
    model = None
    if args.model_pkl:
        filename = copy_from_hdfs(args.model_pkl)
        model = pkl.load(open(filename))
        os.remove(filename)
    elif args.model_proto:
        filename = copy_from_hdfs(args.model_proto)
        model = LOPQModel.load_proto(args.model_proto)
        os.remove(filename)

    print 'LOPQModel is of type: {}'.format(type(model))

    # Load data
    d = data_load_fn(sc, args.data, args.sampling_ratio, args.seed)

    # Deprecated. Now assume m.value.predict will apply PCA if needed
    # # Apply PCA before encoding if needed
    # if args.pca_model is not None:
    #     # Check if we should get PCA model
    #     print 'Loading PCA model from {}'.format(args.pca_model)
    #     filename = copy_from_hdfs(args.pca_model)
    #     params = pkl.load(open(filename))
    #     # TODO: we should also remove tmp dir
    #     os.remove(filename)
    #     P = params['P']
    #     mu = params['mu']
    #     print 'Applying PCA from model {}'.format(args.pca_model)
    #     # Use mapValues this time as we DO have the ids as keys
    #     d = d.mapValues(lambda x: apply_PCA(x, mu, P))

    # Distribute model instance
    m = sc.broadcast(model)

    # Compute codes and convert to string
    codes = d.map(lambda x: (x[0], m.value.predict(x[1]))).map(lambda x: '%s\t%s' % (x[0], json.dumps(x[1])))

    codes.saveAsTextFile(args.output)


if __name__ == "__main__":
    from argparse import ArgumentParser
    parser = ArgumentParser()

    # Data handling parameters
    parser.add_argument('--data', dest='data', type=str, default=None, required=True, help='hdfs path to input data')
    parser.add_argument('--data_udf', dest='data_udf', type=str, default=None, help='module name from which to load a data loading UDF')
    parser.add_argument('--seed', dest='seed', type=int, default=None, help='optional random seed for sampling')
    parser.add_argument('--sampling_ratio', dest='sampling_ratio', type=float, default=1.0, help='proportion of data to sample for model application')
    parser.add_argument('--output', dest='output', type=str, default=None, required=True, help='hdfs path to output data')
    # Deprecated.
    #parser.add_argument('--pca_model', dest='pca_model', type=str, default=None, help='hdfs path to pickle file containing PCA model to be used')

    existing_model_group = parser.add_mutually_exclusive_group(required=True)
    existing_model_group.add_argument('--model_pkl', dest='model_pkl', type=str, default=None, help='a pickled LOPQModel to evaluate on the data')
    existing_model_group.add_argument('--model_proto', dest='model_proto', type=str, default=None, help='a protobuf LOPQModel to evaluate on the data')

    args = parser.parse_args()

    sc = SparkContext(appName='LOPQ code computation')

    # Load UDF module if provided
    if args.data_udf:
        sc.addPyFile('hdfs://memex/user/skaraman/build-lopq-index/lopq/spark/memex_udf.py')
        sc.addPyFile('hdfs://memex/user/skaraman/build-lopq-index/lopq/spark/deepsentibanktf_udf_wid.py')
        udf_module = __import__(args.data_udf, fromlist=['udf'])
        load_udf = udf_module.udf
        main(sc, args, data_load_fn=load_udf)
    else:
        main(sc, args)

    sc.stop()
