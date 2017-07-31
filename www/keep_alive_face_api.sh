#!/bin/bash

sleep_time=10

CONF_FILE="/home/ubuntu/memex/ColumbiaFaceSearch/conf/global_conf_facesearch_lopq_pretrained_docker_v2.json"
LOG_FOLDER="/home/ubuntu/memex/data/logs/"
echo "LOG_FOLDER:" ${LOG_FOLDER}
mkdir -p ${LOG_FOLDER}
API_FOLDER="/home/ubuntu/memex/ColumbiaFaceSearch/www/"
API_TYPE="run_face_search_api"
LOG_KEEP_ALIVE="logFaceAPI_keep_alive.txt"
LOG_API="logFaceAPI$(date +%Y-%m-%d).txt"

while true;
do
    if [ ${CONF_FILE+x} ];
    then {
        echo "["$(date)"] Using conf file: "${CONF_FILE} >> ${LOG_FOLDER}${LOG_KEEP_ALIVE};
        cd ${API_FOLDER}
        python ${API_FOLDER}${API_TYPE}.py -c ${CONF_FILE} &> ${LOG_FOLDER}${LOG_API};
    }
    else {
       echo "["$(date)"] Using default conf file." >> ${LOG_FOLDER}${LOG_KEEP_ALIVE};
       python ${API_FOLDER}${API_TYPE}.py &> ${LOG_FOLDER}${LOG_API};
    }
    fi
    echo "["$(date)"] API crashed." >> ${LOG_FOLDER}${LOG_KEEP_ALIVE};
    sleep ${sleep_time};
done
