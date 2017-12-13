#!/bin/bash

PORT_HOST=80

repo_path=$(git rev-parse --show-toplevel)
indocker_repo_path=/home/ubuntu/memex/ColumbiaImageSearch
base_docker_image="columbia_image_search"
docker_image_tag="1.0"
PORT_DOCKER=5000
ports_mapping="-p "${PORT_HOST}":"${PORT_DOCKER}
s3credentials_path=${repo_path}"/conf/aws_credentials/"
indocker_s3credentials_path="/home/ubuntu/.aws/"
docker_name="dlibface_search"
start_script=${indocker_repo_path}"/setup/DLibFaceSearch/run_dlib_face_search.sh"

# TODO: check trick to run docker without sudo i.e. add it to sudoers group
SUDO='sudo'

## Check/build docker
${SUDO} bash ${repo_path}/setup/DockerBuild/build_docker_columbia_image_search.sh

## Start docker
echo "Starting docker "${docker_name}" from image "${base_docker_image}":"${docker_image_tag}
${SUDO} docker stop ${docker_name}
${SUDO} docker rm ${docker_name}
${SUDO} docker run ${ports_mapping} -tid -v ${s3credentials_path}:${indocker_s3credentials_path} -v ${repo_path}:${indocker_repo_path} --cap-add IPC_LOCK --name=${docker_name} ${base_docker_image}:${docker_image_tag}

## Start process
echo "Starting DLib Face Search"
${SUDO} docker exec -itd ${docker_name} bash ${start_script} -r ${indocker_repo_path}