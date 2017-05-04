### SETUP

This folder contains the files necessary to build the docker containers and compile the tools that are needed to run the image search and the precomputation of similar images.

## Prerequisite

You need to have docker installed in the 'host' machine.
Both docker files expect the existence of an 'update' folder, itself containing an 'indexing' folder with the list of indexed images.
This 'indexing' folder should contain four subfolders:
-  comp_features
-  comp_idx
-  features
-  hash_bits
and two files:
-  sha1_mapping  
-  update_list_dev.txt 

The image search docker can leverage a GPU that can be used to compute the features of the images that will be indexed later on by batch.

## Precomputation of similar images

Go in folder 'precomp' and follow the instructions there.

## Search images

Go in folder 'search' and follow the instructions there.
