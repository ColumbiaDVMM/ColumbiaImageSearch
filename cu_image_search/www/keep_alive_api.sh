#/bin/bash

# set CONF_FILE from argument if any
#CONF_FILE=$1
# or assume environment variable

echo ${CONF_FILE}

while true;
do
    if [ ${CONF_FILE+x} ]; then {
        echo "Using conf file: "${CONF_FILE};
        python api.py -c ${CONF_FILE} &> logAPI$(date +%Y-%m-%d).txt;
    } else {
       echo "Using default conf file.";
       python api.py &> logAPI$(date +%Y-%m-%d).txt;
    }
    fi
    echo "["$(date)"] API crashed." >> logAPI_keep_alive.txt;
    sleep 5;
done
