#!/bin/bash
# TODO: set this, test or release?
suffix="_test"
#suffix="_release"

package_name="cufacesearch"
extr_type="sbpycaffe"
conf="conf_extr_"${extr_type}${suffix}".json"
# Should these two script be run on different machines?
# extraction_checker could be run on the same machine as the search API? or as the image downloader one?
python ../../${package_name}/${package_name}/updater/extraction_checker.py -t -d -c ../../conf/${conf} &> log_check${suffix}_${extr_type}_$(date +%Y-%m-%d).txt &
python ../../${package_name}/${package_name}/updater/extraction_processor.py -t -c ../../conf/${conf} &> log_proc${suffix}_${extr_type}_$(date +%Y-%m-%d).txt


