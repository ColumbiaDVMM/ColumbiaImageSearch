# Columbia University Image and Face Search Tool

Author: [Svebor Karaman](mailto:svebor.karaman@columbia.edu)

This repository implements the image and face search tools developed 
by the [DVMM lab](http://www.ee.columbia.edu/ln/dvmm/) of Columbia University for the 
[MEMEX project](https://www.darpa.mil/program/memex) by Dr. Svebor Karaman, Dr. Tao Chen and Prof. Shih-Fu Chang.

## Overview

This project can be used to build a searchable index of images that can scale to millions of images.
It provides a RESTful API for querying the index to find similar images in less than a second.

The required processing to build the image or face search index can be 
decomposed in the following three main steps:

- images ingestion, that could be:
  - reading from a directory of images 
  - or downloading images (contained in webpages) from a data source 
of webpages documents (currently assumed to be in the MEMEX CDR v3.1 format in a Kafka topic);
- image processing: perform the detection and feature extraction on those images;
- images indexing: build a search index (currently using a modified version of [LOPQ](https://github.com/yahoo/lopq)) and expose 
it through a REST API.

The full image recognition model is based on the DeepSentibank feature representation 
that was trained targeting the Adjective-Noun Pairs (ANP) of the 
[Visual Sentiment Ontology](http://www.ee.columbia.edu/ln/dvmm/vso/download/sentibank.html).

The face detection and recognition model are currently the publicly available models from the [DLib](http://blog.dlib.net/) library, 
see the blog post [DLib face recognition](http://blog.dlib.net/2017/02/high-quality-face-recognition-with-deep.html) 
for more information about the models. 

However, the package `cufacesearch` has been written in a modular way and using 
another image feature extraction model, face detection or recognition model should be fairly easy.

NB: For now, the python package is still named `cufacesearch` even if it contains both 
image and face search capability. The package will be renamed soon.

[//]: # (Add a figure overview)

## Installation 

### Pre-requisite

The current implementation assumes it can have access to both HBase tables and Kafka topics.
If you do not currently have access to those resources you can set up a testing environment 
with docker by following the guidelines in [Prerequisite](./setup/Prerequisite/README.md).

NB: This is currently still a work in progress as connecting to Kafka set up as this does not seem to work yet.

### Configure settings

The whole pipeline can be configure from a single settings file. 
An example setting is provided in [settings.env.sample](./setup/settings.env.sample).

You should (copy and) edit this file to match your target settings.
These settings are roughly divided into:

- HBase settings: you should provide the host, and a table name for storing infos about the images, and updates (batch of images to be processed)
- Kafka settings: for the security keys, the path are relative to the root of this repository and should be in a folder in it.
- Input settings: folder containing the images or Kafka topic containing the ads.
- Extraction settings: wether to perform face or image processing.
- Search settings: parameters of the indexing and API. 

BEWARE: Note that the Kafka topics you define have to be created manually in your Kafka manager 
before starting any processing. 


### Generate configuration files

Once you have set up your settings file, you can generate the configuration files by running the command: 

`./setup/ConfGenerator/generate_confs.sh -s ./setup/settings.env.sample`

### Start the dockers

Any of the following scripts will first build the needed docker image. 
But as they share the same docker image, it will be done only once.

Once the configuration file have been generated, you can start the ingestion with: 
`./scripts/start_docker_local_images_pusher.sh -s ./setup/settings.env.sample`
It will generate log files starting with `log_image_ingestion_[conf_name]`. 
Check these logs for any error.  

You can then start the processing with: 
`./scripts/start_docker_processing.sh -s ./setup/settings.env.sample`
It will generate log files starting with `log_check_[conf_name]` and `log_proc_[conf_name]`. 
Check these logs for any error.  

Finally, you can start the indexing and search with: 
`./scripts/start_docker_search.sh -s ./setup/settings.env.sample`
It will generate log files starting with `log_search_[conf_name]`. 
Check these logs for any error.

### Perform searches

You can check the readmes [DLibFaceSearch](./setup/DLibFaceSearch/README.md) and 
[SentibankImageSearch](./setup/SentibankPyCaffeImageSearch/README.md) for details about the APIs.
A sample `python` script is provided at the end of each README.

You can also open your browser at `http://localhost/[endpoint]/view_similar_byURL?data=[an_image_URL]` to visualize some results. 

## License

Apache License Version 2.0, see [LICENSE](LICENSE).