#/bin/bash

sleep_time=10

# set CONF_FILE from argument if any
#CONF_FILE=$1
# or assume environment variable

echo "CONF_FILE:" ${CONF_FILE}

while true;
do
    if [ ${CONF_FILE+x} ]; then {
        echo "["$(date)"] Using conf file: "${CONF_FILE} >> logAPI_keep_alive.txt;
        python api.py -c ${CONF_FILE} &> logAPI$(date +%Y-%m-%d).txt;
    } else {
       echo "["$(date)"] Using default conf file." >> logAPI_keep_alive.txt;
       #python api.py &> logAPI$(date +%Y-%m-%d).txt;
    }
    fi
    echo "["$(date)"] API crashed." >> logAPI_keep_alive.txt;
    sleep ${sleep_time};
done
