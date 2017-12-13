# Image pusher setup 

## Start script

The script [start_docker_local_images_pusher.sh](start_docker_local_images_pusher.sh) will build the docker 
image from the docker file [DockerFileColumbiaImageSearch](../DockerBuild/DockerFileColumbiaImageSearch), 
setup all dependencies (using the script 
[setup_columbia_image_search.sh](../DockerBuild/setup_columbia_image_search.sh)) 
and start pushing images found in the folder `LIPK_input_path` to the Kafka topic `KID_producer_images_out_topic`.

Once the docker is started and the pushing images you will see a
log file with a filename starting with `log_image_pusher` appear in this folder.
If the docker is running but this log file is not created, it should mean you 
did not properly configure some paths in the scripts as detailed below. 
You should also check this log file for any error in your configuration.  

### Script parameters
You should edit the following parameter in the script:

- `suffix` in the script [run_local_images_pusher.sh](run_local_images_pusher.sh): 
corresponding to whatever suffix (e.g. `test`) you use for your configuration file. 

## Configuration file

You should edit the Kafka related parameters in your config file (e.g. [conf_local_images_pusher_test.json](../../conf/conf_local_images_pusher_test.json)), 
to make sure that:

- the input directory `LIKP_input_path` contains the images that you want to index. A relative path with regards to the 
folder containing this README.md is OK. And subfolders should be OK too.
- the servers lists `LIKP_producer_servers` are pointing to the correct addresses 
of your Kafka brokers.
- the certificates listed in `LIKP_producer_security` to connect to the 
Kafka broker are available at the location  defined in the config file. 
The relative path is with regards to the folder containing this README.md.
- the output topic `LIKP_producer_images_out_topic` should have been created 
beforehand if the Kafka configuration has the parameter `auto.create.topics.enable` set to `False`.
Otherwise you would get an error like `kafka.errors.KafkaTimeoutError: KafkaTimeoutError: Failed to update metadata`. 

## TODO

Add the ability to define the valid extensions in the configuration file.
Currently relying on the variable `valid_formats = ['JPEG', 'JPG', 'GIF', 'PNG']` in 
[local_images_kafka_pusher.py](../../cufacesearch/cufacesearch/ingester/local_images_kafka_pusher.py)

Can be tested with a relatively small dataset like [Caltech101](http://www.vision.caltech.edu/Image_Datasets/Caltech101/101_ObjectCategories.tar.gz).