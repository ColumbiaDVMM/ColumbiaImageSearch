#!/bin/bash

# Get configuration and extraction type
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

cd ${repo_path}

# Initialize path environment variables
source ~/.bashrc

package_name="cufacesearch"

# Start extraction checker
echo "Start extraction checker"
cmd="python ./"${package_name}"/"${package_name}"/updater/extraction_checker.py"
args="-d -c ./conf/generated/conf_extraction_"${conf_name}".json"
log="log_check_"${conf_name}
bash ./scripts/keep_alive_process.sh --cmd="${cmd}" --args="${args}" --log="${log}"&

# Start extraction processor
echo "Start extraction processor"
cmd="python ./"${package_name}"/"${package_name}"/updater/extraction_processor.py"
args="-c ./conf/generated/conf_extraction_"${conf_name}".json"
log="log_proc_"${conf_name}
bash ./scripts/keep_alive_process.sh --cmd="${cmd}" --args="${args}" --log="${log}"

