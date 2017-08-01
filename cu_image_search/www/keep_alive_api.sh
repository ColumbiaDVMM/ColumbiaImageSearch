#!/bin/bash

sleep_time=10

# set CONF_FILE from argument if any
#CONF_FILE=$1
# or assume environment variable

# For Summer QPR
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
echo "CONF_FILE:" ${CONF_FILE}
# log folder should be data/log
LOG_FOLDER="/home/ubuntu/memex/update/logs/"
echo "LOG_FOLDER:" ${LOG_FOLDER}
mkdir -p ${LOG_FOLDER}
API_FOLDER="/home/ubuntu/memex/ColumbiaImageSearch/cu_image_search/www/"
#API_TYPE="api_lopq"
API_TYPE="api"

while true;
do
    if [ ${CONF_FILE+x} ];
    then {
        echo "["$(date)"] Using conf file: "${CONF_FILE} >> ${LOG_FOLDER}logAPI_keep_alive.txt;
        cd ${API_FOLDER}
        python ${API_FOLDER}${API_TYPE}.py -c ${CONF_FILE} &> ${LOG_FOLDER}logAPI$(date +%Y-%m-%d).txt;
    }
    else {
        echo "No conf file set! Leaving"
        exit -1
        #echo "["$(date)"] Using default conf file." >> ${LOG_FOLDER}logAPI_keep_alive.txt;
        #python ${API_FOLDER}${API_TYPE}.py -c ${CONF_FILE} &> ${LOG_FOLDER}logAPI$(date +%Y-%m-%d).txt;
        #python api.py &> logAPI$(date +%Y-%m-%d).txt;
    }
    fi
    echo "["$(date)"] API crashed." >> ${LOG_FOLDER}logAPI_keep_alive.txt;
    sleep ${sleep_time};
done
