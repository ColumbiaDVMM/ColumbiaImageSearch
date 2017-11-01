#!/bin/bash
# TODO: set this, test or release?
#  should this suffix be set from a parameter?
suffix="_test"
#suffix="_release"

source ~/.bashrc

# NB: here 'repo_path' has to be the 'indocker_repo_path'

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

cd ${repo_path}/setup/SentibankPyCaffeImageProcessing

package_name="cufacesearch"
extr_type="sbpycaffe"
conf="conf_extr_"${extr_type}${suffix}".json"

# Start extraction checker
echo "Start extraction checker"
cmd="python ../../"${package_name}"/"${package_name}"/updater/extraction_checker.py"
args="-d -c ../../conf/"${conf}
log="log_check"${suffix}"_"${extr_type}
bash ../../scripts/keep_alive_process.sh --cmd="${cmd}" --args="${args}" --log="${log}"&

# Start extraction processor
echo "Start extraction processor"
cmd="python ../../"${package_name}"/"${package_name}"/updater/extraction_processor.py"
args="-c ../../conf/"${conf}
log="log_proc"${suffix}"_"${extr_type}
bash ../../scripts/keep_alive_process.sh --cmd="${cmd}" --args="${args}" --log="${log}"