This folder contains scripts that can be used to run each component and are called when starting 
docker containers with docker-compose in the [setup](../setup) folder.

* [run_images_pusher.sh](run_images_pusher.sh): can be used to push images from a local folder or
to ingest images from a Kafka topic. It should be modified and tested to ingest from a Kinesis 
stream but should require minimal changes.
 * [run_processing.sh](run_processing.sh): can be used to start a processing pipeline to extract
 features from images.
 * [run_search.sh](run_search.sh): can be used to start a search API. 