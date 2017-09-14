# Docker installation

## Start script

The script [start_docker_sentibank_pycaffe_image_processing..sh](start_docker_sentibank_pycaffe_image_processing.sh) will build the docker image from the docker file, 
setup all dependencies (using the script [setup_sentibank_pycaffe_image_processing.sh](setup_sentibank_pycaffe_image_processing.sh)) and start the processing script.  

You can later on use this script to restart the processing, it will not rebuild the docker image if it is already there.

You should edit the following parameters:

- repo path (or just base_path) in the script [start_docker_sentibank_pycaffe_image_processing..sh](start_docker_sentibank_pycaffe_image_processing.sh): absolute path to the root of this repository. 
- suffix in the script [run_sentibank_pycaffe_image_processing.sh](run_sentibank_pycaffe_image_processing.sh): wether you are runnign in test or release mode.
- nb_threads in the config file ([conf_extr_sbpycaffe_test.json](../../conf/conf_extr_sbpycaffe_test.json) or [conf_extr_sbpycaffe_release.json](../../conf/conf_extr_sbpycaffe_release.json) to the number of cpus available for the image processing. 

Make sure the certificates to connect to the Kafka broker are also available at the location defined in the config file.
