#!/bin/bash

# Get configuration and endpoint
# Get configuration and extraction type
while getopts c:e:r: option
do
  case "${option}"
  in
  c) conf_name=${OPTARG};;
  e) endpoint=${OPTARG};;
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

if [ ${endpoint+x} ]; then
  echo "endpoint: "${endpoint}
else
  echo "endpoint not set. Use -e to set endpoint please."
  exit -1
fi

cd ${repo_path}

# Initialize path environment variables
source ~/.bashrc

# Make sure path are consistent by moving to repo root.
repo_path=$(git rev-parse --show-toplevel)
cd ${repo_path}

# Start and keep API alive
cmd="python ./www/run_search_api.py"
args="-c ./conf/generated/conf_search_"${conf_name}".json -e "${endpoint}
log="log_searchapi_"${endpoint}
bash ./scripts/keep_alive_process.sh --cmd="${cmd}" --args="${args}" --log="${log}"

