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

pip install --user --upgrade pip
pip install --user setuptools
pip install --user --upgrade numpy
pip install -e ${repo_path}/cufacesearch
pip install -e ${repo_path}/lopq


#NB: The lopq package is a work-in-progress modification of LOPQ from the repo [https://github.com/yahoo/lopq](https://github.com/yahoo/lopq), see also [https://github.com/ColumbiaDVMM/ColumbiaImageSearch/tree/master/workflows/build-lopq-index/lopq](https://github.com/ColumbiaDVMM/ColumbiaImageSearch/tree/master/workflows/build-lopq-index/lopq).
#The modifications aim to better integrate the PCA pre-processing, but is actually not yet used in the current version of the search index.
