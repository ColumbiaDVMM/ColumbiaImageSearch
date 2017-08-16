# Columbia University Face Search package

Author: [Svebor Karaman](mailto:svebor.karaman@columbia.edu)

This package is the key component of the face search tool developed for the [MEMEX project](https://www.darpa.mil/program/memex).

The package is divided into multiple modules:

- api: Flask API to expose the face search index.
- detector: detect faces in images.
- featurizer: compute a discriminative feature from a face bounding box.
- imgio: common methods to download images and get images infos.
- indexer: interaction with a database (currently HBase) to get new images and images metadata.
- searcher: search indexing scheme (currently LOPQ).

The face detection and recognition models are currently the publicly available models from the [DLib](http://blog.dlib.net/) library, 
see the blog post [DLib face recognition](http://blog.dlib.net/2017/02/high-quality-face-recognition-with-deep.html) 
for more information about the models. 
However, this package has been written in a modular way and using 
another face detection or recognition model should be fairly easy.
Similarly, the search indexing scheme could be changed.

