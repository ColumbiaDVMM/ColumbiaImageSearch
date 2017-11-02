#!/bin/bash

# NB: here 'repo_path' has to be the 'indocker_repo_path'

while getopts r: option
do
  case "${option}"
  in
  r) repo_path=${OPTARG};;
  esac
done

if [ ${repo_path+x} ]; then
  echo "repo_path: "${repo_path}
else
  echo "repo_path not set. Use -r to set repo_path please."
  exit -1
fi

repodirname="cufacesearch"

pip install --user --upgrade pip
pip install --user setuptools
pip install --user --upgrade numpy
pip install --user protobuf
# -e is for development mode, so changes in the python codes are applied without needing to reinstall
pip install -e ${repo_path}/${repodirname}
pip install -e ${repo_path}/lopq

# Build caffe
echo "Installing caffe (this may take a while)"
caffe_base_path="/home/ubuntu/"
caffe_dir="caffe_cpu"
caffe_path=${caffe_base_path}${caffe_dir}
caffe_repo="https://github.com/BVLC/caffe"
caffe_commit="b963008a6591600e60ed6746d208e82e107f6a89"
cd ${caffe_base_path}; git clone ${caffe_repo} ${caffe_dir}; cd ${caffe_dir}; git reset --hard ${caffe_commit}
# For command line sentibank extraction
cp ${repo_path}/${repodirname}/${repodirname}/featurizer/data/extract_nfeatures.cpp ${caffe_path}/tools
# Use flag CPU_ONLY?
cd ${caffe_path}; mkdir build; cd build; cmake ..; make all; make pycaffe -j8;

echo "export PYTHONPATH=$PYTHONPATH:${caffe_path}/python" >> ~/.bashrc

