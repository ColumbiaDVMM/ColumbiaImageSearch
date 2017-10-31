#!/bin/bash

sleep_time=10

while getopts c:a:l: option
do
  case "${option}"
  in
  c) command=${OPTARG};;
  a) args=${OPTARG};;
  l) log=${OPTARG};;
  esac
done

if [ ${command+x} ]; then
  echo "command: "${command}
else
  echo "command not set. Use -c to set command please."
  exit -1
fi

if [ ${args+x} ]; then
  echo "args: "${args}
else
  echo "args not set. Use -a to set args please."
  exit -1
fi

if [ ${log+x} ]; then
  echo "log: "${log}
else
  echo "log not set. Use -l to set args please."
  exit -1
fi

while true;
do
    echo "["$(date)"] Starting process." >> ${log}"_keepalive";
    ${command} ${args} &> ${log}"_"$(date +%Y-%m-%d);
    echo "["$(date)"] Process crashed." >> ${log}"_keepalive";
    sleep ${sleep_time};
done
