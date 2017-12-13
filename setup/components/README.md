## Components

This folder contains three subfolders corresponding to the three separate components used in the release configuration of the system for the HT domain.

* The `ingest` component reads webpages documents from a Kafka topic looking for associated images and push them to an image topic.
* The `process` component is used to run the extraction of features of the images read from the Kafka topic.
* The `search` component sets up an image search API.

Default configuration files are provided.