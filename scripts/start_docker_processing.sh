#!/bin/bash

while getopts s: option
do
  case "${option}"
  in
  s) settings_file=${OPTARG};;
  esac
done

if [ ${settings_file+x} ]; then
  echo "settings_file: "${settings_file}
else
  echo "settings_file not set. Use -s to set settings_file please."
  exit -1
fi

set -a; . ${settings_file}; set +a

docker_name=${conf_name}"_proc"
base_docker_image="columbia_image_search"
docker_image_tag="1.0"
indocker_repo_path=/home/ubuntu/memex/ColumbiaImageSearch
start_script=${indocker_repo_path}"/scripts/run_processing.sh"

# TODO: check trick to run docker without sudo i.e. add it to sudoers group
# https://github.com/sindresorhus/guides/blob/master/docker-without-sudo.md
SUDO='sudo'

## Check/build docker
repo_path=$(git rev-parse --show-toplevel)
${SUDO} bash ${repo_path}/setup/DockerBuild/build_docker_columbia_image_search.sh

## Start docker
echo "Starting docker "${docker_name}" from image "${base_docker_image}":"${docker_image_tag}
${SUDO} docker stop ${docker_name}
${SUDO} docker rm ${docker_name}
${SUDO} docker run -tid -v ${repo_path}:${indocker_repo_path} --cap-add IPC_LOCK --name=${docker_name} ${base_docker_image}:${docker_image_tag}

## Start process
echo "Starting processing with script "${run_script}
${SUDO} docker exec -itd ${docker_name} bash ${start_script} -r ${indocker_repo_path} -c ${conf_name}
