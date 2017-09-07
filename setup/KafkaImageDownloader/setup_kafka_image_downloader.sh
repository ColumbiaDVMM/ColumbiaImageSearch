#!/bin/bash

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
pip install -e ${repo_path}/${repodirname}


