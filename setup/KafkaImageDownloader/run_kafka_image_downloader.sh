#!/bin/bash
# TODO: set this, test or release?
suffix="_test"
#suffix="_release"

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

cd ${repo_path}/setup/KafkaImageDownloader

python ../../scripts/ingestion/run_image_ingestion.py -t -d -c ../../conf/conf_kafka_image_downloader${suffix}.json &> log_image_ingestion${suffix}_$(date +%Y-%m-%d).txt


