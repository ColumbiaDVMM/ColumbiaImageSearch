#!/bin/bash
# TODO: set this, test or release?
#  should this suffix be set from a parameter?
#suffix="_test"
#suffix="_release"
#suffix="_lfw_local" # Cannot connect to local Kafka, using either "127.0.0.1:9092", "0.0.0.0:9092" or "localhost:9092"
suffix="_lfw"
# NB: here 'repo_path' has to be the 'indocker_repo_path'

while getopts c:r: option
do
  case "${option}"
  in
  c) conf=${OPTARG};;
  r) repo_path=${OPTARG};;
  esac
done

if [ ${repo_path+x} ]; then
  echo "repo_path: "${repo_path}
else
  echo "repo_path not set. Use -r to set repo_path please."
  exit -1
fi

if [ ${repo_path+x} ]; then
  echo "repo_path: "${repo_path}
else
  echo "repo_path not set. Use -r to set repo_path please."
  exit -1
fi

cd ${repo_path}/setup/LocalImagesKafkaPusher

# TODO: write main script and setup config
#python ../../cufacesearch/cufacesearch/ingester/local_images_kafka_pusher.py -c ../../conf/conf_local_images_pusher${suffix}.json &> log_image_ingestion${suffix}_$(date +%Y-%m-%d).txt
cmd="python ../../cufacesearch/cufacesearch/ingester/local_images_kafka_pusher.py"
#cmd="python -m ../../cufacesearch/cufacesearch/ingester/local_images_kafka_pusher"
args="-c ../../conf/conf_local_images_pusher"${suffix}".json"
log="log_image_ingestion"${suffix}
bash ../../scripts/keep_alive_process.sh --cmd="${cmd}" --args="${args}" --log="${log}"
#python ../../scripts/ingestion/run_image_ingestion.py -t -d -c ../../conf/conf_kafka_image_downloader${suffix}.json &> log_image_ingestion${suffix}_$(date +%Y-%m-%d).txt


