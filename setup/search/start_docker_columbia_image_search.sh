#!/bin/bash

with_cuda=false

# Example:
# ./start_docker_columbia_image_search.sh -d /Users/svebor/Documents/Workspace/CodeColumbia/Datasets/Caltech101 -c ../../conf/global_var_sample_local.json -n caltech101 -p 80


# use arguments for the following values:
# - port
# - domain_name
# - data_path
while getopts d:n:p:c: option
do
  case "${option}"
  in
  d) DATA_PATH=${OPTARG};;
  n) DOMAIN_NAME=${OPTARG};;
  p) PORT=${OPTARG};;
  c) CONF=${OPTARG};;
  esac
done

if [ ${DATA_PATH+x} ]; then
  echo "DATA_PATH: "${DATA_PATH}
else
  echo "DATA_PATH not set. Use -d to set DATA_PATH please."
  exit -1
fi
if [ ${CONF+x} ]; then
  echo "CONF: "${CONF}
else
  echo "CONF not set. Use -c to set CONF please."
  exit -1
fi
if [ ${DOMAIN_NAME+x} ]; then
  echo "DOMAIN_NAME: "${DOMAIN_NAME}
else
  echo "DOMAIN_NAME not set. Use -n to set DOMAIN_NAME please."
  exit -1
fi
if [ ${PORT+x} ]; then
  echo "PORT: "${PORT}
else
  echo "PORT not set. Use -p to set PORT please."
  exit -1
fi

## Variables that could be changed
docker_image="cuimgsearch"
docker_image_build_tag="0.1"
docker_image_cuda_tag="0.2"
docker_image_tag="1.0"
docker_name="cuimgsearch_"${DOMAIN_NAME}
docker_file="DockerfileColumbiaImageSearch"

indocker_repo_path=/home/ubuntu/memex/ColumbiaImageSearch/
indocker_data_path=/home/ubuntu/memex/data/
indocker_update_path=/home/ubuntu/memex/update/

if (( $with_cuda ));
then
  nvidia_install_dir="/srv/NVIDIA"
  docker_nvidia_devices="--device /dev/nvidia0:/dev/nvidia0 --device /dev/nvidiactl:/dev/nvidiactl --device /dev/nvidia-uvm:/dev/nvidia-uvm"
else
  nvidia_install_dir=""
  docker_nvidia_devices=""
fi

ports_mapping="-p 80:5000"
repo_path=$(dirname $(dirname $(pwd)))
echo "repo_path is:"${repo_path}
update_path=${repo_path}/update_${DOMAIN_NAME}

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
    ${SUDO} docker build -t ${docker_image}:${docker_image_build_tag} -f ${docker_file} .
}

# build if needed
testDockerExists
echo ${docker_exists}
if [[ ${docker_exists} -eq 0 ]];
then
    echo "Building docker image "${docker_image}" from docker file: "${docker_file}
	buildDocker

    if (( $with_cuda )); # Not fully tested
    then
        # cuda and NVIDIA drivers have to be installed in the same way in the docker than in the host.
        # I was using the run shell script to install cuda 8.0 and the package nvidia-375
        # Should we stop docker container that could be running from build first?
        echo "Please install cuda now"
        ${SUDO} docker run ${ports_mapping} ${docker_nvidia_devices} -ti -v ${repo_path}:${indocker_repo_path} -v ${nvidia_install_dir}:/home/ubuntu/setup_cuda -v ${DATA_PATH}:${indocker_data_path} --cap-add IPC_LOCK --name=${docker_name} ${docker_image}:${docker_image_build_tag} /bin/bash

        # Commit
        ${SUDO} docker commit ${docker_name} ${docker_image}:${docker_image_cuda_tag}
        docker_image_build_tag=${docker_image_cuda_tag}
    fi

    ${SUDO} docker run ${ports_mapping} ${docker_nvidia_devices} -itd -v ${repo_path}:${indocker_repo_path}  -v ${DATA_PATH}:${indocker_data_path} --cap-add IPC_LOCK --name=${docker_name} ${docker_image}:${docker_image_build_tag} /bin/bash

    # Setup environment
    ${SUDO} docker exec -it ${docker_name} ${indocker_repo_path}/setup/search/setup_search.sh
    # Commit
    ${SUDO} docker commit ${docker_name} ${docker_image}:${docker_image_tag}

else
	echo "Docker image "${docker_image}" already built."
fi

## Start docker
echo "Starting docker "${docker_name}" from image "${docker_image}":"${docker_image_tag}

${SUDO} docker stop ${docker_name}
${SUDO} docker rm ${docker_name}

# no need for NVIDIA directory after install
#docker run ${ports_mapping} ${docker_nvidia_devices} -ti -v ${repo_path}:/home/ubuntu/memex/ColumbiaImageSearch -v/srv/NVIDIA:/home/ubuntu/setup_cuda -v ${search_update_path}:/home/ubuntu/memex/update --cap-add IPC_LOCK --name=${docker_name} ${docker_image}:${docker_image_tag} /bin/bash
${SUDO} docker run ${ports_mapping} ${docker_nvidia_devices} -itd -v ${update_path}:${indocker_update_path}  -v ${repo_path}:${indocker_repo_path} -v ${DATA_PATH}:${indocker_data_path} --cap-add IPC_LOCK --name=${docker_name} ${docker_image}:${docker_image_tag} /bin/bash

# Start update process
#${SUDO} docker exec -itd ${docker_name} ${indocker_repo_path}/scripts/run_update.sh -c ${indocker_repo_path}${CONF} && sleep 5 && ${indocker_repo_path}/cu_image_search/www/keep_alive_api.sh -c ${indocker_repo_path}${CONF}
${SUDO} docker exec -itd ${docker_name} ${indocker_repo_path}/scripts/run_update.sh -c ${indocker_repo_path}${CONF}

# Run API
${SUDO} docker exec -itd ${docker_name} ${indocker_repo_path}/cu_image_search/www/keep_alive_api.sh -c ${indocker_repo_path}${CONF}