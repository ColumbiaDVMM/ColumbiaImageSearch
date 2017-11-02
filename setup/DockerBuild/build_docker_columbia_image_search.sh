#!/bin/bash

# You should not need to change anything in that script,
# and if you do, make sure that the config file and other scripts reflects these changes

repo_path=$(git rev-parse --show-toplevel)
indocker_repo_path=/home/ubuntu/memex/ColumbiaImageSearch
base_docker_image="columbia_image_search"
docker_image_tag="1.0"
docker_image_build_tag="0.9"
setup_dir=${repo_path}"/setup/DockerBuild/"
docker_file=${setup_dir}"DockerFileColumbiaImageSearch"
setup_script=${indocker_repo_path}"/setup/DockerBuild/setup_columbia_image_search.sh"
setup_docker_name="cu_img_search"

SUDO='sudo'

## TODO: try to push/load from docker hub

## Build the docker image if needed

# test base docker i.e. columbia_image_search:0.9
testBaseDockerExists() {
    cmd_test_docker="${SUDO} docker images | grep ${base_docker_image}[' ']*${docker_image_build_tag} | wc -l"
    echo ${cmd_test_docker}
    base_docker_exists=$(${SUDO} docker images | grep "${base_docker_image}[' ']*${docker_image_build_tag}"  | wc -l)
}

# test docker columbia_image_search:1.0 existence
testDockerExists() {
    cmd_test_docker="${SUDO} docker images | grep ${base_docker_image}[' ']*${docker_image_tag} | wc -l"
    echo ${cmd_test_docker}
    docker_exists=$(${SUDO} docker images | grep "${base_docker_image}[' ']*${docker_image_tag}"  | wc -l)
}

## Build docker image from docker file
testBuildBaseDocker() {
    testBaseDockerExists

    echo "Base docker exists: "${base_docker_exists}
    if [[ ${docker_exists} -eq 0 ]];
    then
        # remember where we are
        run_dir=$(pwd)
        # we have to be in the directory containing the docker_file to build the docker image
        docker_dir=$(dirname ${docker_file})
        cd ${docker_dir}
        echo "Building docker image "${base_docker_image}":"${docker_image_build_tag}" from docker file: "${docker_file}
        ${SUDO} docker build -t ${base_docker_image}:${docker_image_build_tag} -f ${docker_file} ${setup_dir}
        # go back to where we were
        cd ${run_dir}
    else
        echo "Base docker image "${base_docker_image}" already built."
    fi
}

## Build if needed
testDockerExists
echo "Docker exists: "${docker_exists}
if [[ ${docker_exists} -eq 0 ]];
then
    testBuildBaseDocker

    echo "Setting up docker image "${base_docker_image}
    # Then we should run setup script
    ${SUDO} docker run -tid -v ${repo_path}:${indocker_repo_path} --cap-add IPC_LOCK --name=${setup_docker_name} ${base_docker_image}:${docker_image_build_tag}
    # Run without detach so we wait for setup to complete
    ${SUDO} docker exec -it ${setup_docker_name} bash ${setup_script} -r ${indocker_repo_path}
    # Commit
    ${SUDO} docker commit ${setup_docker_name} ${base_docker_image}:${docker_image_tag}

else
	echo "Docker image "${base_docker_image}" already built."
fi

${SUDO} docker stop ${setup_docker_name}

# Push?