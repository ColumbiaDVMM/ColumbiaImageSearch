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

cd ${repo_path}/setup/SentibankPyCaffeImageProcessing

package_name="cufacesearch"
extr_type="sbpycaffe"
conf="conf_extr_"${extr_type}${suffix}".json"
# Should these two script be run on different machines?
# extraction_checker could be run on the same machine as the search API? or as the image downloader one?
python ../../${package_name}/${package_name}/updater/extraction_checker.py -d -c ../../conf/${conf} &> log_check${suffix}_${extr_type}_$(date +%Y-%m-%d).txt &
python ../../${package_name}/${package_name}/updater/extraction_processor.py -c ../../conf/${conf} &> log_proc${suffix}_${extr_type}_$(date +%Y-%m-%d).txt


