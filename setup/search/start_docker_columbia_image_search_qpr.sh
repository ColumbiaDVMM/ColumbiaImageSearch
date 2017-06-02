#!/bin/bash

with_cuda=false

# for different domains we could dynamically name the docker_name
# use arguments for the following values?
# - domain
# - port
# - update_path

while getopts d:p: option
do
  case "${option}"
  in
  d) DOMAIN=${OPTARG};;
  p) PORT=${OPTARG};;
  esac
done

if [ ${DOMAIN+x} ]; then
  echo "DOMAIN: "${DOMAIN}
else
  echo "Domain not set. Use -d to set domain please."
  exit -1
fi
if [ ${PORT+x} ]; then
  echo "PORT: "${PORT}
else
  echo "Port not set. Use -p to set port please."
  exit -1
fi

repo_path=/home/ubuntu/ColumbiaImageSearch/
update_path=/home/ubuntu/data_${DOMAIN}/

# how to create the different global_conf files for the different domains?

## Variables that could be changed
docker_image="columbiaimagesearch"
#docker_image_tag="0.8" # build 0.8, install cuda if needed, run setup_search.sh, commit as 0.9
docker_image_tag="0.9"
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
ports_mapping="-p "${PORT}":5000"

# # That is dependent on the path where the script is called from...
# repo_path=$(dirname $(dirname $(pwd)))
# echo "repo_path is:"${repo_path}

## Docker requires sudo privilege, check if we already have them
# this seems to fail on OpenStack where ubuntu has sudo privilege but cannot interact with docker without sudo
#SUDO=''
#if (( $EUID != 0 )); then
#    SUDO='sudo'
#fi
SUDO='sudo'

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

${SUDO} docker stop ${docker_name}
${SUDO} docker rm ${docker_name}

## We should store that for re-use
#echo -n 'Please enter update path: '
#read search_update_path

# no need for NVIDIA directory after install
#docker run ${ports_mapping} ${docker_nvidia_devices} -ti -v ${repo_path}:/home/ubuntu/memex/ColumbiaImageSearch -v/srv/NVIDIA:/home/ubuntu/setup_cuda -v ${search_update_path}:/home/ubuntu/memex/update --cap-add IPC_LOCK --name=${docker_name} ${docker_image}:${docker_image_tag} /bin/bash
#${SUDO} docker run ${ports_mapping} ${docker_nvidia_devices} -ti -v ${repo_path}:/home/ubuntu/memex/ColumbiaImageSearch -v ${update_path}:/home/ubuntu/memex/update --cap-add IPC_LOCK --name=${docker_name} ${docker_image}:${docker_image_tag} /bin/bash
${SUDO} docker run ${ports_mapping} ${docker_nvidia_devices} -tid -v ${repo_path}:/home/ubuntu/memex/ColumbiaImageSearch -v ${update_path}:/home/ubuntu/memex/update --cap-add IPC_LOCK --name=${docker_name} ${docker_image}:${docker_image_tag}

