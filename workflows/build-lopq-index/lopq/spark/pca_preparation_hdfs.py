# Copyright 2015, Yahoo Inc.
# Licensed under the terms of the Apache License, Version 2.0. See the LICENSE file associated with the project for terms.

"""
This script illustrates how to prepare PCA parameters before using them in the LOPQ pipeline.

The `pca_params` argument is the path to a pickle file containing PCA parameters like that 
produced as a result of `train_pca.py`, and the `D` argument is the desired dimension of the
final feature. This script truncates then permutes the dimensions of the PCA matrix to balance
the variance across the the two halves of the final vector.
"""
import os
import base64
import subprocess
import numpy as np
import cPickle as pkl
from tempfile import NamedTemporaryFile, mkdtemp
from lopq.model import eigenvalue_allocation


def copy_from_hdfs(hdfs_path):
    tmp_dir = mkdtemp()
    subprocess.call(['hadoop', 'fs', '-copyToLocal', hdfs_path, tmp_dir])
    return os.path.join(tmp_dir, hdfs_path.split('/')[-1])


def copy_to_hdfs(f, hdfs_path):
    subprocess.call(['hadoop', 'fs', '-copyFromLocal', f.name, hdfs_path])


def main(args):

    # assume hdfs path in params
    filename = copy_from_hdfs(args.pca_params)
    print 'Loading PCA Model locally from {} copied from {}'.format(filename, args.pca_params)
    params = pkl.load(open(filename))
    os.remove(filename)

    P = params['P']
    E = params['E']
    mu = params['mu']

    # Reduce dimension - eigenvalues assumed in ascending order
    E = E[-args.D:]
    P = P[:,-args.D:]

    # Balance variance across halves
    permuted_inds = eigenvalue_allocation(2, E)
    P = P[:, permuted_inds]

    # Save new params
    f = NamedTemporaryFile(delete=False)
    pkl.dump({'P': P, 'mu': mu }, open(f.name, 'w'))
    f.close()
    copy_to_hdfs(f, args.output)
    os.remove(f.name)



def apply_PCA(x, mu, P):
    """
    Example of applying PCA.
    """
    return np.dot(x - mu, P)


if __name__ == '__main__':
    from argparse import ArgumentParser
    parser = ArgumentParser()

    parser.add_argument('--pca_params', dest='pca_params', type=str, required=True, help='path to pickle file of PCA parameters')
    parser.add_argument('--D', dest='D', type=int, default=128, help='desired final feature dimension')
    parser.add_argument('--output', dest='output', type=str, required=True, help='path to pickle file of new PCA parameters')
    args = parser.parse_args()

    main(args)
