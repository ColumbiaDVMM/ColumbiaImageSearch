#!/bin/bash

# NB: here 'repo_path' has to be the 'indocker_repo_path'

while getopts c:r: option
do
  case "${option}"
  in
  c) conf_name=${OPTARG};;
  r) repo_path=${OPTARG};;
  esac
done

if [ ${conf_name+x} ]; then
  echo "conf_name: "${conf_name}
else
  echo "conf_name not set. Use -c to set conf_name please."
  exit -1
fi

if [ ${repo_path+x} ]; then
  echo "repo_path: "${repo_path}
else
  echo "repo_path not set. Use -r to set repo_path please."
  exit -1
fi

if [ ${input_type+x} ]; then
  echo "input_type: "${input_type}
else
  echo "input_type not set. Please set the environment variable: 'input_type'"
  exit -1
fi

cd ${repo_path}

package_name="cufacesearch"

if [ "$input_type" = "local" ];
then
    cmd="python ./"${package_name}"/"${package_name}"/ingester/local_images_kafka_pusher.py"
    args=" -c ./conf/generated/conf_ingestion_"${conf_name}.json
else
    if [ ${nb_workers+x} ]; then
      echo "nb_workers: "${nb_workers}
    else
      echo "nb_workers not set. Please set the environment variable: 'nb_workers'"
      # Should we assume nb_workers=1?
      exit -1
    fi
    cmd="python ./"${package_name}"/"${package_name}"/ingester/kafka_image_downloader.py"
    args=" -t -d -w ${nb_workers} -c ./conf/generated/conf_ingestion_"${conf_name}.json
fi
#mkdir "./logs"
#log="./logs/log_image_ingestion_"${conf_name}
#bash ./scripts/keep_alive_process.sh --cmd="${cmd}" --args="${args}" --log="${log}"
bash ./scripts/keep_alive_process.sh --cmd="${cmd}" --args="${args}"
