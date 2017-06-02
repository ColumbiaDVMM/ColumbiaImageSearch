#!/bin/bash

sleep_time=10

# set CONF_FILE from argument if any
#CONF_FILE=$1
# or assume environment variable

# For Summer QPR
CONF_FILE="/home/ubuntu/memex/update/data/global_var_remotehbase_release.json"
echo "CONF_FILE:" ${CONF_FILE}
# log folder should be data/log
LOG_FOLDER="/home/ubuntu/memex/update/logs/"
echo "LOG_FOLDER:" ${LOG_FOLDER} 
mkdir -p ${LOG_FOLDER}

while true;
do
    if [ ${CONF_FILE+x} ]; then {
        echo "["$(date)"] Using conf file: "${CONF_FILE} >> ${LOG_FOLDER}logAPI_keep_alive.txt;
        python api.py -c ${CONF_FILE} &> logAPI$(date +%Y-%m-%d).txt;
    } else {
       echo "["$(date)"] Using default conf file." >> ${LOG_FOLDER}logAPI_keep_alive.txt;
       #python api.py &> logAPI$(date +%Y-%m-%d).txt;
    }
    fi
    echo "["$(date)"] API crashed." >> ${LOG_FOLDER}logAPI_keep_alive.txt;
    sleep ${sleep_time};
done
