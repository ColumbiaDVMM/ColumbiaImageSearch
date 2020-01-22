# Columbia University Image and Face Search Tool

Author: [Svebor Karaman](mailto:svebor.karaman@columbia.edu)

This repository implements the image and face search tools developed 
by the [DVMM lab](http://www.ee.columbia.edu/ln/dvmm/) of Columbia University for the 
[MEMEX project](https://www.darpa.mil/program/memex) by 
Dr. Svebor Karaman, Dr. Tao Chen and Prof. Shih-Fu Chang.

## Overview

This project can be used to build a searchable index of images that can scale to millions of images.
It provides a RESTful API for querying the index to find similar images in less than a second.

The images index is built by extracting features from the images. 
Two feature extraction models are included:

* A full image recognition model is based on the DeepSentibank feature representation 
that was trained targeting the Adjective-Noun Pairs (ANP) of the 
[Visual Sentiment Ontology](http://www.ee.columbia.edu/ln/dvmm/vso/download/sentibank.html).
* A face detection and recognition model, that are the publicly available models from the 
[DLib](http://blog.dlib.net/) 
library, see the blog post 
[DLib face recognition](http://blog.dlib.net/2017/02/high-quality-face-recognition-with-deep.html) 
for more information about the models. 

However, the package `cufacesearch` has been written in a modular way and using 
another image feature extraction model, face detection or recognition model should be fairly easy.
The package `cufacesearch` is fully documented, you can build the documentation following the 
instructions in the [docs](cufacesearch/docs) folder.

NB: For now, the python package is still named `cufacesearch` even if it contains both 
image and face search capability. The package could be renamed soon.

[//]: # (Add a figure overview, from the website?)

## Installation 

### Pre-requisite

This repository relies on docker and docker-compose for an easy setup, you will need to have those installed.
Install docker-compose on your system following the guidelines at: https://docs.docker.com/compose/install/.

You could install all the dependencies packages and run the tools outside of docker, but this is considered 
an advanced setting that is not documented yet.

### Setup the environment

The folder [setup](./setup) contains detailed description on how to setup the tool, with examples building the index 
for publicly available datasets. Check the [README.md](./setup/README.md) in that folder to get you started. 

### Perform searches

You can check the [README.md](./www/README.md) file in `www` folder for details about the API usage.
You can also open your browser at `http://localhost/[endpoint]/view_similar_byURL?data=[an_image_URL]` 
to visualize some results. 

## License and citations

This software is release under the Apache License Version 2.0, see [LICENSE](LICENSE).
This repository contains a modified version of the 
[python lopq package](https://github.com/yahoo/lopq/tree/master/python) in [lopq](lopq) also
released under an Apache License Version 2.0.

If you use this software in a product please mention in any communication 
(website, presentation, etc...) regarding the image search capabilities of your product 
that it relies on this repository with a sentence like: 


> The image search capabilities of this product rely on the open-sourced "ColumbiaImageSearch" 
> tool developed by Dr. Svebor Karaman, Dr. Tao Chen and Prof. Shih-Fu Chang at the 
> [DVMM lab](http://www.ee.columbia.edu/ln/dvmm/) of Columbia University and available 
> at https://github.com/ColumbiaDVMM/ColumbiaImageSearch.
 
If you use this repository in a research paper, you can cite it as:

```
@misc{KaramanCIS2015,
  author = {Svebor Karaman and Tao Chen and Shih{-}Fu Chang},
  title = {Columbia Image Search},
  year = {2015},
  publisher = {GitHub},
  journal = {GitHub repository},
  howpublished = {\url{https://github.com/ColumbiaDVMM/ColumbiaImageSearch}}
}
```

If you use the DeepSentibank feature extraction, please cite the following paper:

```
@article{ChenDSB14,
  author    = {Tao Chen and
               Damian Borth and
               Trevor Darrell and
               Shih{-}Fu Chang},
  title     = {DeepSentiBank: Visual Sentiment Concept Classification with Deep Convolutional
               Neural Networks},
  journal   = {CoRR},
  volume    = {abs/1410.8586},
  year      = {2014},
  url       = {http://arxiv.org/abs/1410.8586}
}
```

## Acknowledgments

This research was supported in part by the Defense Advanced Research Projects Agency (DARPA) and the Air Force Research Laboratory (AFRL) under contract number FA8750-14-C-0240. The views and conclusions contained herein are those of the authors and should not be interpreted as necessarily representing the official policies or endorsements, either expressed or implied, of DARPA or the U.S. Government.


## Contact

Please feel free to contact [me](mailto:svebor.karaman@columbia.edu) with any questions you may have.
Also, please post any issue you encounter or request features on github.