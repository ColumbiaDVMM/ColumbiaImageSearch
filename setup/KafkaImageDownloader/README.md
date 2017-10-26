# Image downloader setup 

## Start script

The script [start_docker_kafka_image_downloader.sh](start_docker_kafka_image_downloader.sh) will build the docker 
image from the docker file, setup all dependencies (using the script 
[setup_kafka_image_downloader.sh](setup_kafka_image_downloader.sh)) and start the image downloading script 
that will download the images of all pages posted in CDR v3.1 format 
in the input topic `KID_consumer_topics` in the configuration file 
and post the pages in a CDR v3.1 extended format that includes 
information about the images to the topic `KID_producer_cdr_out_topic` 
and the image information to another topic `KID_producer_images_out_topic`.

Once the docker is started and the downloading process is running you will see a
log file with a filename starting with `log_image_ingestion` appear in this folder.
If the docker is running but this log file is not created, it should mean you 
did not properly configure some paths in the scripts as detailed below. 
You should also check this log file for any error.  

### Script parameters
You should edit the following parameters in the script:

- repo path (or just base_path) in the script [start_docker_kafka_image_downloader.sh](start_docker_kafka_image_downloader.sh): 
absolute path to the root of this repository. 
- `suffix` in the script [run_sentibank_pycaffe_image_processing.sh](run_sentibank_pycaffe_image_processing.sh): 
corresponding to whatever suffix (e.g. `test`) you use for your configuration file. 
In the same script you might want to change the `nb_workers` parameter to a lower number to reduce the CPU usage.

## Configuration file

You should edit the Kafka related parameters in your config file (e.g. [conf_kafka_image_downloader_test.json](../../conf/conf_kafka_image_downloader_test.json)), 
to make sure that:

- the servers lists `KID_consumer_servers` and 
`KID_producer_servers` are pointing to the correct addresses 
of your Kafka brokers.
- the certificates listed in 
`KID_consumer_security` and `KID_producer_security`
to connect to the Kafka broker are available at the location 
defined in the config file. 
The relative path is with regards to the folder containing this README.md.
- the output topics `KID_producer_cdr_out_topic` and 
`KID_producer_images_out_topic` have been created 
beforehand, they will NOT be created automatically.

You may lower the parameter `KID_nb_threads` to decrease the CPU usage. 
You may decrease/increase the parameter `KID_verbose` in the range [0,6] to get less/additional information in 
the log file.