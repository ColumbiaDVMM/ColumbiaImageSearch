# Columbia University Image and Face Search Tool

Author: [Svebor Karaman](mailto:svebor.karaman@columbia.edu)

This repository implements the image and face search tools developed 
by the [DVMM lab](http://www.ee.columbia.edu/ln/dvmm/) of Columbia University for the 
[MEMEX project](https://www.darpa.mil/program/memex) by Dr. Svebor Karaman, Dr. Tao Chen and Prof. Shih-Fu Chang.

## Overview

The required processing to build am image search index can be 
decomposed in three main steps:

- images downloading: download images (contained in webpages) from a data source 
where the webpages documents are in the MEMEX CDR v3.1 format (currently assumed to be a Kafka topic)
- image processing: perform the detection and feature extraction on those images.
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

### Docker installation

For convenience a docker installation process is provided in [setup](./setup),
there is one sub-folder for each step of a processing pipeline. 
The first downloading step being shared by the two pipelines, there are 5 sub-folders. 
Check the README.md in those setup folder for additional information.

### Configuration

Most of the settings can be configured through a JSON file 
passed as parameter of one of the processing steps.
Some examples configuration files are provided in the [conf](conf) folder.
Additional detail are provided in each sub-folder of the [setup](./setup) folder.

## License

Apache License Version 2.0, see [LICENSE](LICENSE).