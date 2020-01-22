# All-in-one setup

The `docker-compose` in this directory will start everything needed for the image processing pipeline at once. 
It is intended to be used as a testing environment but should not be used in a production system that would require
HBase and Kafka/Kinesis to be properly set up on a cluster.

The docker compose files in this folder will start multiple docker containers on the host machine, 
including an HBase and Kafka/Kinesis instance. The image processing pipeline is divided into an images pusher, an images processor
and an images search container. 

## Environment files

A single environment file is used to set all the parameters of the processing pipeline.
Four examples enviromnent file are provided:
  - `.env.caltech101local.kafka` and `.env.caltech101local.kinesis`: this pipeline will download and index the dataset [Caltech101](http://www.vision.caltech.edu/Image_Datasets/Caltech101/) using the Sentibank features relying on either `kafka` or `kinesis` for ingestion. 
  - `.env.lfwlocal.kafka` and `.env.lfwlocal.kinesis`: this pipeline will download and index the [LFW](http://vis-www.cs.umass.edu/lfw/) dataset using the DLib face detection and features relying on either `kafka` or `kinesis` for ingestion.
   
Copy (and edit) one of the example environment file to `.env` in this folder.

The menaning of the environment file parameters are detailed in each component readme file.


## Docker compose files

Docker compose files 
[docker-compose_kafka.yml](docker-compose_kafka.yml) and
[docker-compose_kinesis.yml](docker-compose_kinesis.yml) for each ingestion method are provided.

Additionally, docker compose files with additional monitoring tools 
[docker-compose_kafka_monitor.yml](docker-compose_kafka_monitor.yml) and
[docker-compose_kinesis_monitor.yml](docker-compose_kinesis_monitor.yml)
are also provided.

You can use the tool by choosing the appropriate docker compose file 
and running the following command:

- `docker-compose -f docker-compose_(kafka/kinesis)[_monitor].yml up`

Or just copy the appropriate docker compose file as and just run:

- `docker-compose up`

The monitoring docker compose files will additionally start a docker container for:
 - [Hue](http://gethue.com): so you can check the tables and data stored in HBase. You should be able to check the HBase tables at [http://localhost:9999/hue/hbase/#hbase](http://localhost:9999/hue/hbase/#hbase)
 - [Kafka-manager](https://github.com/yahoo/kafka-manager): so you can check the Kafka topics and consumers, 
 it should be accessible at [http://localhost:9997/kafka_manager/](http://localhost:9997/kafka_manager/). 
 However, by default no Kafka clusters will be available. You need to add your cluster based on the settings of the environment file you use.

## Check status

You can check all docker logs to monitor the processing status. 
The API is ready to be used when you see a message like `Starting Search API on port 5000`.
You can check the API status by querying the route `status` of the `endpoint` you defined, 
e.g. [http://localhost/cufacesearch/status](http://localhost/cufacesearch/status).
Details about the API are provided in the [README.md](../../www/README.md) file of the `www` folder. 
You can also open your browser at [http://localhost/[endpoint]/view_similar_byURL?data=[an_image_URL]](http://localhost/[endpoint]/view_similar_byURL?data=[an_image_URL]) to visualize some results.

