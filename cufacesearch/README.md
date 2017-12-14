# Columbia University Iamge and Face Search package

Author: [Svebor Karaman](mailto:svebor.karaman@columbia.edu)

This package is the key component of the image and face search tool developed for the [MEMEX project](https://www.darpa.mil/program/memex).

The package is divided into multiple sub-modules:

- [api](./api): Flask API to expose the face search index.
- [common](./common): some common resources, like configuration reader, error printing.
- [detector](./detector): detect faces in images.
- [extractor](./extractor): generic extraction process defined by a detector and featurizer.
- [featurizer](./featurizer): compute a discriminative feature from a face bounding box.
- [imgio](./imgio): common methods to download images and get images infos.
- [indexer](./indexer): interaction with a database (currently HBase) to get new images and images metadata.
- [ingester](./ingester): ingest images from a data source (currently local folder or Kafka).
- [searcher](./searcher): search indexing scheme (currently LOPQ).
- [storer](./storer): store data locally or to S3.
- [updater](./updater): check for new images to be processed and process them.