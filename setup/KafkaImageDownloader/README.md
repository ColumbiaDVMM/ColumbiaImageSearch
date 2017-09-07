# Docker installation

## Start script

The script [start_docker_kafka_image_downloader.sh](start_docker_kafka_image_downloader.sh) will build the docker 
image from the docker file, setup all dependencies (using the script 
[setup_kafka_image_downloader.sh](setup_kafka_image_downloader.sh)) and start the image downloading script 
that will download the images of all pages posted in CDR v3 format in the input topic and post the pages
in a CDR v3 extended format that includes information about the images to one topic and the raw image buffer to another topic.  

You should edit the following parameters in the script:

- repo_path: absolute path to the root of this repository. 
- data_path: absolute path to the folder containing the face data.
- config_file:
