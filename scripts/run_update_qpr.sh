#!/bin/bash
start_date=$(date +%Y-%m-%d)

CONF_FILE="/home/ubuntu/memex/update/data/global_var_remotehbase_release.json"
echo "CONF_FILE:" ${CONF_FILE}
# log folder should be data/log
LOG_FOLDER="/home/ubuntu/memex/update/logs/"
echo "LOG_FOLDER:" ${LOG_FOLDER}
mkdir -p ${LOG_FOLDER}
SCRIPT_FOLDER="/home/ubuntu/memex/ColumbiaImageSearch/scripts/"

python ${SCRIPT_FOLDER}continuous_update_hbase.py ${CONF_FILE} &> ${LOG_FOLDER}log_update_${start_date}.txt
