#!/bin/bash

sleep_time=60

while getopts c: option
do
  case "${option}"
  in
  c) CONF=${OPTARG};;
  esac
done

if [ ${CONF+x} ]; then
  echo "Received option CONF: "${CONF}
  # For QPR Summer 2017 should be
  # CONF_FILE="/home/ubuntu/memex/update/data/global_var_summerqpr2017.json"
  CONF_FILE=${CONF}
else
  echo "CONF not set. Using default configuration file."
  CONF_FILE="/home/ubuntu/memex/update/data/global_var_remotehbase_release.json"
fi

start_date=$(date +%Y-%m-%d)
LOG_FOLDER="/home/ubuntu/memex/update/logs/"
SCRIPT_FOLDER="/home/ubuntu/memex/ColumbiaImageSearch/scripts/"
while true;
do
    python ${SCRIPT_FOLDER}continuous_update_hbase.py ${CONF_FILE}  &> ${LOG_FOLDER}log_update_${start_date}.txt
    echo "["$(date)"] Update crashed." >> ${LOG_FOLDER}log_update_keep_alive.txt;
    sleep ${sleep_time}
done
