# Docker installation

## Start script

The script [start_docker_kafka_image_downloader.sh](start_docker_kafka_image_downloader.sh) will build the docker 
image from the docker file, setup all dependencies (using the script 
[setup_kafka_image_downloader.sh](setup_kafka_image_downloader.sh)) and start the image downloading script 
that will download the images of all pages posted in CDR v3 format in the input topic and post the pages
in a CDR v3 extended format that includes information about the images to one topic and the raw image buffer to another topic.  

You should edit the following parameters in the script:

- repo path (or just base_path) in the script [start_docker_kafka_image_downloader.sh](start_docker_kafka_image_downloader.sh): absolute path to the root of this repository. 
- suffix in the script [run_sentibank_pycaffe_image_processing.sh](run_sentibank_pycaffe_image_processing.sh): wether you are runnign in test or release mode.
- potentially nb_threads in the config file ([conf_kafka_image_downloader_test.json](../../conf/conf_kafka_image_downloader_test.json) or [conf_kafka_image_downloader_release.json](../../conf/conf_kafka_image_downloader_test.json), it is the number of thread per worker. It seems OK to have more threads total than the number of cpus.

Make sure the certificates to connect to the Kafka broker are also available at the location defined in the config file.
