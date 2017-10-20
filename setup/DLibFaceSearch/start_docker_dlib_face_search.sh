#!/bin/bash

## Adjust that to the actual host values
# TODO: set this, base_path on host machine
#base_path=/media/data/Code/MEMEX/
#base_path=~
base_path=/srv/skaraman/

# You should not need to change that,
# and if you do, make sure that the config file reflects these changes

repo_path=${base_path}/columbiafacesearch/
PORT_HOST=81
PORT_DOCKER=5000
ports_mapping="-p "${PORT_HOST}":"${PORT_DOCKER}
indocker_repo_path=/home/ubuntu/memex/ColumbiaImageSearch
s3credentials_path=${repo_path}/conf/aws_credentials/
indocker_s3credentials_path=/home/ubuntu/.aws/

## Variables that could be changed
docker_image="dlibface_search"

docker_image_tag="1.0"
docker_image_build_tag="0.9"
docker_name="dlibface_search"
setup_str="DLibFaceSearch"
docker_file=${repo_path}"/setup/"${setup_str}"/DockerFile"${setup_str}
setup_script=${indocker_repo_path}"/setup/"${setup_str}"/setup_dlib_face_search.sh"
start_script=${indocker_repo_path}"/setup/"${setup_str}"/run_dlib_face_search.sh"

SUDO='sudo'

## Build the docker image if needed
# test docker image existence
testDockerExists() {
    cmd_test_docker="${SUDO} docker images | grep ${docker_image}[' ']*${docker_image_tag} | wc -l"
    echo ${cmd_test_docker}
    docker_exists=$(${SUDO} docker images | grep "${docker_image}[' ']*${docker_image_tag}"  | wc -l)
}

## Build docker image from docker file
buildDocker() {
    # we have to be in the directory containing the docker_file
    run_dir=$(pwd)
    docker_dir=$(dirname ${docker_file})
    cd ${docker_dir}
    ${SUDO} docker build -t ${docker_image}:${docker_image_build_tag} -f ${docker_file} .
    # go back to run dir
    cd ${run_dir}
}

## Build if needed
testDockerExists
echo "Docker exists: "${docker_exists}
if [[ ${docker_exists} -eq 0 ]];
then
  echo "Building docker image "${docker_image}" from docker file: "${docker_file}
  buildDocker

  echo "Setting up docker image "${docker_image}

  # Then we should run setup_face_search.sh
  ${SUDO} docker run -tid -v ${repo_path}:${indocker_repo_path} --cap-add IPC_LOCK --name=${docker_name} ${docker_image}:${docker_image_build_tag}
  # Run without detach so we wait for setup to complete
  ${SUDO} docker exec -it ${docker_name} bash ${setup_script} -r ${indocker_repo_path}

  # Commit
  ${SUDO} docker commit ${docker_name} ${docker_image}:${docker_image_tag}

else
	echo "Docker image "${docker_image}" already built."
fi

## Start docker
echo "Starting docker "${docker_name}" from image "${docker_image}":"${docker_image_tag}

${SUDO} docker stop ${docker_name}
${SUDO} docker rm ${docker_name}

## Start API
${SUDO} docker run ${ports_mapping}  -tid -v ${repo_path}:${indocker_repo_path} -v ${s3credentials_path}:${indocker_s3credentials_path} --cap-add IPC_LOCK --name=${docker_name} ${docker_image}:${docker_image_tag}
echo "Starting DLib Face Search"
${SUDO} docker exec -itd ${docker_name} bash ${start_script} -r ${indocker_repo_path}
