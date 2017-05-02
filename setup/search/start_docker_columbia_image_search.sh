#!/bin/bash

## Variables that could be changed
docker_image="columbiaimagesearch"
docker_image_tag="0.1"
docker_name="columbia_university_search_similar_images"
docker_file="DockerfileColumbiaImageSearch"
#docker_nvidia_devices="--device /dev/nvidia0:/dev/nvidia0 --device /dev/nvidiactl:/dev/nvidiactl --device /dev/nvidia-uvm:/dev/nvidia-uvm"
# while testing without an actual GPU
docker_nvidia_devices=""
ports_mapping="-p 85:5000"
repo_path=$(dirname $(dirname $(pwd)))
echo "repo_path is:"${repo_path}

## Docker requires sudo privilege, check if we already have them
SUDO=''
if (( $EUID != 0 )); then

    SUDO='sudo'
fi

## Build the docker image if needed
# test docker image existence
testDockerExists() {
    #cmd_test_docker="${SUDO} docker images | grep ${docker_image} | grep ${docker_image_tag} | wc -l"
    #echo ${cmd_test_docker}
    docker_exists=$(${SUDO} docker images | grep ${docker_image} | grep ${docker_image_tag} | wc -l)
}

# build docker image from docker file
buildDocker() {
    ${SUDO} docker build -t ${docker_image}:${docker_image_tag} -f ${docker_file} .
}

# build if needed
testDockerExists
echo ${docker_exists}
if [[ ${docker_exists} -eq 0 ]];
then
        echo "Building docker image "${docker_image}" from docker file: "${docker_file}
	buildDocker
else
	echo "Docker image "${docker_image}" already built."
fi

## Start docker
echo "Starting docker "${docker_name}" from image "${docker_image}":"${docker_image_tag}

docker stop ${docker_name}
docker rm ${docker_name}

# We should store that for re-use
echo -n 'Please enter update path: '
read search_update_path

docker run ${ports_mapping} ${docker_nvidia_devices} -ti -v ${repo_path}:/home/ubuntu/memex/ColumbiaImageSearch -v ${search_update_path}:/home/ubuntu/memex/update --cap-add IPC_LOCK --name=${docker_name} ${docker_image}:${docker_image_tag} /bin/bash
