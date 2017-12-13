#!/bin/bash

repo_path=$(git rev-parse --show-toplevel)
indocker_repo_path=/home/ubuntu/memex/ColumbiaImageSearch
base_docker_image="columbia_image_search"
docker_image_tag="1.0"

docker_name="local_img_push"
start_script=${indocker_repo_path}"/setup/LocalImagesKafkaPusher/run_local_images_pusher.sh"

# TODO: check trick to run docker without sudo i.e. add it to sudoers group
SUDO='sudo'

## Check/build docker
${SUDO} bash ${repo_path}/setup/DockerBuild/build_docker_columbia_image_search.sh

## Start docker
echo "Starting docker "${docker_name}" from image "${base_docker_image}":"${docker_image_tag}
${SUDO} docker stop ${docker_name}
${SUDO} docker rm ${docker_name}
${SUDO} docker run -tid -v ${repo_path}:${indocker_repo_path} --cap-add IPC_LOCK --name=${docker_name} ${base_docker_image}:${docker_image_tag}

## Start process
echo "Starting Local Images Pusher"
${SUDO} docker exec -itd ${docker_name} bash ${start_script} -r ${indocker_repo_path}
