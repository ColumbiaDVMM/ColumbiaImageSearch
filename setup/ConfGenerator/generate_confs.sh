#!/bin/bash

while getopts s: option
do
  case "${option}"
  in
  s) settings_file=${OPTARG};;
  esac
done

if [ ${settings_file+x} ]; then
  echo "settings_file: "${settings_file}
else
  echo "settings_file not set. Use -s to set settings_file please."
  exit -1
fi

set -a; . ${settings_file}; set +a

repo_path=$(git rev-parse --show-toplevel)
cd ${repo_path}/setup/ConfGenerator
conf_dir=${repo_path}/conf/generated/
mkdir ${conf_dir}
python ./create_conf_ingester.py -o ${conf_dir}
python ./create_conf_extractor.py -o ${conf_dir}
python ./create_conf_searcher.py -o ${conf_dir}