##!/bin/bash
#
### Adjust that to the actual host values
## TODO: set this, base_path on host machine
#repo_path=$(git rev-parse --show-toplevel)
#
## You should not need to change that,
## and if you do, make sure that the config file reflects these changes
#indocker_repo_path=/home/ubuntu/memex/ColumbiaImageSearch
#
### Variables that could be changed
#docker_image="sentibank_pycaffe_image_processing"
#
#docker_image_tag="1.0"
#docker_image_build_tag="0.9"
#docker_name="sb_pycaffe_img_proc"
#docker_file=${repo_path}"/setup/SentibankPyCaffeImageProcessing/DockerFileSentibankPyCaffeImageProcessing"
#setup_script=${indocker_repo_path}"/setup/SentibankPyCaffeImageProcessing/setup_sentibank_pycaffe_image_processing.sh"
#start_script=${indocker_repo_path}"/setup/SentibankPyCaffeImageProcessing/run_sentibank_pycaffe_image_processing.sh"
#
#SUDO='sudo'
#
### Build the docker image if needed
## test docker image existence
#testDockerExists() {
#    cmd_test_docker="${SUDO} docker images | grep ${docker_image}[' ']*${docker_image_tag} | wc -l"
#    echo ${cmd_test_docker}
#    docker_exists=$(${SUDO} docker images | grep "${docker_image}[' ']*${docker_image_tag}"  | wc -l)
#}
#
### Build docker image from docker file
#buildDocker() {
#    # we have to be in the directory containing the docker_file
#    run_dir=$(pwd)
#    docker_dir=$(dirname ${docker_file})
#    cd ${docker_dir}
#    ${SUDO} docker build -t ${docker_image}:${docker_image_build_tag} -f ${docker_file} .
#    # go back to run dir
#    cd ${run_dir}
#}
#
### Build if needed
#testDockerExists
#echo "Docker exists: "${docker_exists}
#if [[ ${docker_exists} -eq 0 ]];
#then
#  echo "Building docker image "${docker_image}" from docker file: "${docker_file}
#  buildDocker
#
#  echo "Setting up docker image "${docker_image}
#
#  # Then we should run setup_face_search.sh
#  ${SUDO} docker run -tid -v ${repo_path}:${indocker_repo_path} --cap-add IPC_LOCK --name=${docker_name} ${docker_image}:${docker_image_build_tag}
#  # Run without detach so we wait for setup to complete
#  ${SUDO} docker exec -it ${docker_name} bash ${setup_script} -r ${indocker_repo_path}
#
#  # Commit
#  ${SUDO} docker commit ${docker_name} ${docker_image}:${docker_image_tag}
#
#else
#	echo "Docker image "${docker_image}" already built."
#fi
#
### Start docker
#echo "Starting docker "${docker_name}" from image "${docker_image}":"${docker_image_tag}
#
#${SUDO} docker stop ${docker_name}
#${SUDO} docker rm ${docker_name}
#
### Start API
#${SUDO} docker run -tid -v ${repo_path}:${indocker_repo_path} --cap-add IPC_LOCK --name=${docker_name} ${docker_image}:${docker_image_tag}
#echo "Starting Sentibank Image Processing"
#${SUDO} docker exec -itd ${docker_name} bash ${start_script} -r ${indocker_repo_path}
#

#!/bin/bash

repo_path=$(git rev-parse --show-toplevel)
indocker_repo_path=/home/ubuntu/memex/ColumbiaImageSearch
base_docker_image="columbia_image_search"
docker_image_tag="1.0"

docker_name="sb_pycaffe_img_proc"
start_script=${indocker_repo_path}"/setup/SentibankPyCaffeImageProcessing/run_sentibank_pycaffe_image_processing.sh"

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
echo "Starting Sentibank Image Processing"
${SUDO} docker exec -itd ${docker_name} bash ${start_script} -r ${indocker_repo_path}
