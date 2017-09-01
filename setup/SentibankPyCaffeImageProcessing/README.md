# Docker installation

## Start script

The script [start_docker_sentibank_pycaffe_image_processing..sh](start_docker_sentibank_pycaffe_image_processing.sh) will build the docker image from the docker file, 
setup all dependencies (using the script [setup_sentibank_pycaffe_image_processing.sh](setup_sentibank_pycaffe_image_processing.sh)) and start the processing script.  

You can later on use this script to restart the processing.

You should edit the following parameters in the script:

- repo_path: absolute path to the root of this repository. 
- data_path: absolute path to the folder containing the face data.
- config_file:
