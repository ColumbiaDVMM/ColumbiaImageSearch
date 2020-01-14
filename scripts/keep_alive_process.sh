#!/bin/bash

sleep_time=10
max_nb_crash=3

while getopts :-: option
do
  case "${option}"
  in
  - )  LONG_OPTARG="${OPTARG#*=}"
         case $OPTARG in
           cmd=?*  ) command="$LONG_OPTARG" ;;
           args=?* ) args="$LONG_OPTARG" ;;
           log=?* ) log="$LONG_OPTARG";;
           *    ) echo "Illegal option --$OPTARG" >&2; exit 2 ;;
         esac ;;
  esac
done

if [ ${command+x} ]; then
  echo "command: "${command}
else
  echo "command not set. Use --cmd to set command please."
  exit -1
fi

if [ ${args+x} ]; then
  echo "args: "${args}
else
  echo "args not set. Use --args to set args please."
  exit -1
fi

# New: have a maximum number of failures
nb_crash=0
#while true;
while [[ $nb_crash < $max_nb_crash ]];
do
    nb_crash=$((nb_crash+1));
    if [ ${log+x} ]; then
        echo "["$(date)"] Starting process." >> ${log}"_keepalive";
        ${command} ${args} &> ${log}"_"$(date +%Y-%m-%d_%H-%M-%S);
        echo "["$(date)"] Process crashed. Crash #"$nb_crash >> ${log}"_keepalive";
    else
        echo "["$(date)"] Starting process.";
        ${command} ${args};
        echo "["$(date)"] Process crashed. Crash #"$nb_crash;
    fi
    echo "["$(date)"] Sleeping for "${sleep_time}" seconds."
    sleep ${sleep_time};
done

# New: if we reach that point, processes have failed multiple times
echo "Command "${command}" has failed multiple times."
exit 1
