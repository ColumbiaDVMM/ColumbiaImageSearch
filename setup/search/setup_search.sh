#!/bin/bash

#NB: This script has to be called WITHIN the docker
install_python_pkgs=1
install_caffe=0
compile_hashing=0

# have a parameter to build caffe with or without gpu support?
with_cuda=false

## Initialization
# get path of repo root
repo_path="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd ../.. && pwd )"
#repo_path=/home/ubuntu/memex/ColumbiaImageSearch/
# make that an independent repo?
path_lopq="workflows/build-lopq-index/lopq/python"

## Python
if [[ $install_python_pkgs -eq 1 ]];
then
	# install required python packages
	echo "Installing python packages"
	apt-get install -y python-cffi
	pip install --user --upgrade pip
	pip install --user setuptools
    pip install --user --upgrade numpy
	# run it twice because it seems to fail the first time sometimes...
	pip install --user -r ${repo_path}/requirements.txt
	pip install --user -r ${repo_path}/requirements.txt
	# we should also install modified lopq version...
	cd ${repo_path}/${path_lopq}
	python setup.py install --user --force
	cd ${repo_path}
fi

## Caffe
if [[ $install_caffe -eq 1 ]];
then
	# assuming nvidia drivers and cuda were properly installed before

	# download version of caffe that is known to work (commit e3c895b https://github.com/BVLC/caffe)
	# a folder /home/ubuntu/caffe_gpu should have been created from the docker file
	echo "Installing caffe (this may take a while)"
	caffe_base_path="/home/ubuntu/"
	caffe_dir="caffe_gpu"
	caffe_path=${caffe_base_path}${caffe_dir}
	caffe_repo="https://github.com/BVLC/caffe"
	caffe_commit="b963008a6591600e60ed6746d208e82e107f6a89"

	cd ${caffe_base_path}; git clone ${caffe_repo} ${caffe_dir}; cd ${caffe_dir}; git reset --hard ${caffe_commit}
	#cd ${caffe_base_path}; cd ${caffe_dir}; git reset --hard ${caffe_commit}

	# copy file to extract multiple features
	cp ${repo_path}/cu_image_search/feature_extractor/sentibank/extract_nfeatures.cpp ${caffe_path}/tools
	# compile
	cd ${caffe_path}; mkdir build; cd build
        cmake ..
        # use flag CPU_ONLY if no cuda was installed
        if (( $with_cuda ));
            make -j8
        then
            make CPU_ONLY=true -j8
        fi

    # create symbolic link
    ln -s ${caffe_path}/build/tools/extract_nfeatures ${repo_path}/cu_image_search/feature_extractor/sentibank/extract_nfeatures_gpu

	# deepsentibank model path will be provided from conf file
fi

## Compile hashing related code
if [[ $compile_hashing -eq 1 ]];
then
	# compile hashing C++ code
	echo "Compiling hashing C++ code"
	cd ${repo_path}/cu_image_search/hashing_new && make;

	# compile python wrapper
	echo "Compiling hashing python wrapper code"
	cd ${repo_path}/cu_image_search/hashing_new/python && ./comp.sh;

	# #echo "Testing hashing python wrapper"
	# #python test_hashobjpy.py
fi
