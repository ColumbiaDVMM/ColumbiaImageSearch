#!/bin/bash

with_cuda=false

# for different domains we could dynamically name the docker_name

## Variables that could be changed
docker_image="columbiaimagesearch"
#docker_image_tag="0.7" # build 0.7, install cuda, run setup_search.sh, commit as 0.8
docker_image_tag="0.8"
docker_name="columbia_university_search_similar_images"
docker_file="DockerfileColumbiaImageSearch"
if (( $with_cuda ));
then
  nvidia_install_dir="/srv/NVIDIA"
  docker_nvidia_devices="--device /dev/nvidia0:/dev/nvidia0 --device /dev/nvidiactl:/dev/nvidiactl --device /dev/nvidia-uvm:/dev/nvidia-uvm"
else
  nvidia_install_dir=""
  docker_nvidia_devices=""
fi
# while testing without an actual GPU
#docker_nvidia_devices=""
#ports_mapping="-p 85:5000"
# to test
#ports_mapping="-p 88:5000"
ports_mapping="-p 80:5000"
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
    cmd_test_docker="${SUDO} docker images | grep ${docker_image} | grep ${docker_image_tag} | wc -l"
    echo ${cmd_test_docker}
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
        if (( $with_cuda ));
        then
	  # cuda and NVIDIA drivers have to be installed in the same way in the docker than in the host.
	  # I was using the run shell script to install cuda 8.0 and the package nvidia-375
	  echo "Please install cuda now"
	  docker run ${ports_mapping} ${docker_nvidia_devices} -ti -v ${repo_path}:/home/ubuntu/memex/ColumbiaImageSearch -v${nvidia_install_dir}:/home/ubuntu/setup_cuda -v ${search_update_path}:/home/ubuntu/memex/update --cap-add IPC_LOCK --name=${docker_name} ${docker_image}:${docker_image_tag} /bin/bash
        fi
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

# no need for NVIDIA directory after install
#docker run ${ports_mapping} ${docker_nvidia_devices} -ti -v ${repo_path}:/home/ubuntu/memex/ColumbiaImageSearch -v/srv/NVIDIA:/home/ubuntu/setup_cuda -v ${search_update_path}:/home/ubuntu/memex/update --cap-add IPC_LOCK --name=${docker_name} ${docker_image}:${docker_image_tag} /bin/bash
docker run ${ports_mapping} ${docker_nvidia_devices} -ti -v ${repo_path}:/home/ubuntu/memex/ColumbiaImageSearch -v ${search_update_path}:/home/ubuntu/memex/update --cap-add IPC_LOCK --name=${docker_name} ${docker_image}:${docker_image_tag} /bin/bash
