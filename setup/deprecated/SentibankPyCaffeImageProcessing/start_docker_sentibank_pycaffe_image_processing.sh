#!/bin/bash

docker_name="sb_pycaffe_img_proc"
run_script="/setup/SentibankPyCaffeImageProcessing/run_sentibank_pycaffe_image_processing.sh"

repo_path=$(git rev-parse --show-toplevel)
indocker_repo_path=/home/ubuntu/memex/ColumbiaImageSearch
base_docker_image="columbia_image_search"
docker_image_tag="1.0"

start_script=${indocker_repo_path}${run_script}

# TODO: check trick to run docker without sudo i.e. add it to sudoers group
# https://github.com/sindresorhus/guides/blob/master/docker-without-sudo.md
SUDO='sudo'


## Check/build docker
${SUDO} bash ${repo_path}/setup/DockerBuild/build_docker_columbia_image_search.sh

## Start docker
echo "Starting docker "${docker_name}" from image "${base_docker_image}":"${docker_image_tag}
${SUDO} docker stop ${docker_name}
${SUDO} docker rm ${docker_name}
${SUDO} docker run -tid -v ${repo_path}:${indocker_repo_path} --cap-add IPC_LOCK --name=${docker_name} ${base_docker_image}:${docker_image_tag}

## Start process
echo "Starting Processing with script "${run_script}
${SUDO} docker exec -itd ${docker_name} bash ${start_script} -r ${indocker_repo_path}
