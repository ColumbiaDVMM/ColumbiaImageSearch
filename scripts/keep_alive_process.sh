#!/bin/bash

sleep_time=60

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

while true;
do
    if [ ${log+x} ]; then
        echo "["$(date)"] Starting process." >> ${log}"_keepalive";
        ${command} ${args} &> ${log}"_"$(date +%Y-%m-%d_%H-%M-%S);
        echo "["$(date)"] Process crashed." >> ${log}"_keepalive";
    else
        echo "["$(date)"] Starting process."
        ${command} ${args}
        echo "["$(date)"] Process crashed."
    fi
    sleep ${sleep_time};
done
