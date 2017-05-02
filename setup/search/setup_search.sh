#!/bin/bash

# install required python packages
echo "Installing python packages"
pip install --upgrade pip
pip install -U setuptools
pip install -U -r ./requirements.txt

# compile hashing C++ code
echo "Compiling hashing C++ code"
cd cu_image_search/hashing_new && make;

# compile python wrapper
echo "Compiling hashing python wrapper code"
cd python && ./comp.sh;


#echo "Testing hashing python wrapper"
#python test_hashobjpy.py

