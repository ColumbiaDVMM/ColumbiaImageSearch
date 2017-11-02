#!/bin/bash
# TODO: set this, test or release?
#  should this suffix be set from a parameter?

suffix="_test"
#suffix="_release"
#suffix="_packathon"
endpoint="cufacesearch"

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

cd ${repo_path}/setup/DLibFaceSearch
source ~/.bashrc


extr_type="cufacesearch"
conf="conf_search_"${extr_type}${suffix}".json"

cmd="python ../../www/run_search_api.py"
args="-c ../../conf/"${conf}"  -e "${endpoint}
log="log_"${extr_type}"_searchapi"${suffix}
bash ../../scripts/keep_alive_process.sh --cmd="${cmd}" --args="${args}" --log="${log}"



#
##!/bin/bash
## TODO: set this, test or release?
##  should this suffix be set from a parameter?
##suffix="_test"
#suffix="_release"
#endpoint="cufacesearch"
##source ~/.bashrc
#
#while getopts r: option
#do
#  case "${option}"
#  in
#  r) repo_path=${OPTARG};;
#  esac
#done
#
#if [ ${repo_path+x} ]; then
#  echo "repo_path: "${repo_path}
#else
#  echo "repo_path not set. Use -r to set repo_path please."
#  exit -1
#fi
#
#cd ${repo_path}/setup/DLibFaceSearch
#
#package_name="cufacesearch"
#extr_type="dlibface"
#conf="conf_search_"${extr_type}${suffix}".json"
## Should these two script be run on different machines?
## extraction_checker could be run on the same machine as the search API? or as the image downloader one?
## Should we have a script that make sure this process are still alive akin to the keep_alive_api.sh scripts...
#
#python ../../www/run_search_api.py -c ../../conf/${conf} -e ${endpoint} &> log_searchapi${suffix}_${extr_type}_$(date +%Y-%m-%d_%H-%M-%S).txt
#
#
#
