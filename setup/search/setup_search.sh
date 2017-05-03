#!/bin/bash

#NB: This script has to be called WITHIN the docker

## Initialization
# get path of repo root
repo_path="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd ../.. && pwd )"

## Caffe
# install cuda
# will download nvidia-375 even if already installed?
#error?
#Preparing to unpack .../nvidia-375_375.51-0ubuntu1_amd64.deb ...
#Failed to connect to bus: No such file or directory
#Removing all DKMS Modules

# echo "Installing cuda (this may take a while)"
# dpkg -i ${repo_path}/setup/search/cuda-repo-ubuntu1404_8.0.61-1_amd64.deb
# apt-get update
# apt-get -y install cuda --no-install-recommends

# download version of caffe that is known to work (commit e3c895b https://github.com/BVLC/caffe)
# a folder /home/ubuntu/caffe_gpu should have been created from the docker file
echo "Installing caffe (this may take a while)"
caffe_base_path="/home/ubuntu/"
caffe_dir="caffe_gpu"
caffe_path=${caffe_base_path}${caffe_dir}
caffe_repo="https://github.com/BVLC/caffe"
caffe_commit="e3c895b"
#cd ${caffe_base_path}; git clone ${caffe_repo} ${caffe_dir}; cd ${caffe_dir}; git reset --hard ${caffe_commit}
cd ${caffe_base_path}; cd ${caffe_dir}; git reset --hard ${caffe_commit}
# copy file to extract multiple features
cp ${repo_path}/cu_image_search/feature_extractor/sentibank/extract_nfeatures.cpp ${caffe_path}/tools
# compile
cd ${caffe_path}; mkdir build; cd build; cmake ..; make -j8

# how do we get the deepsentibank model?

# test sentibank?

# ## Python
# # install required python packages
# echo "Installing python packages"
# pip install --upgrade pip
# pip install -U setuptools
# pip install -U -r ${repo_path}/requirements.txt

# ## Compile hashing related code
# # compile hashing C++ code
# echo "Compiling hashing C++ code"
# cd ${repo_path}/cu_image_search/hashing_new && make;

# # compile python wrapper
# echo "Compiling hashing python wrapper code"
# cd ${repo_path}/cu_image_search/hashing_new/python && ./comp.sh;


# #echo "Testing hashing python wrapper"
# #python test_hashobjpy.py

