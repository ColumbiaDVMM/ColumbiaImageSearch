# Image Search Service Overview

This component enables the construction of an efficient search index based on [LOPQ](../../../lopq) 
and expose the index through a rest API to enable searching for similar images at scale.

We detail below:
 * what are the parameters of the environment file 
 * how the search index is trained and used
 * how to start and check the status of the search component with `docker-compose` 

## Environment File

Multiple example environment files are listed in this folder, the most recent ones being 
[.env_merged_sb](.env_merged_sb) and [.env_merged_dlib](.env_merged_dlib) which should be 
good starting point for your own configuration. 
The environment file parameters are passed to the docker container that generates a JSON
 configuration file that is then loaded by the different python classes to adapt their parameters
 to the provided configuration. 
 See the line `command` in the [docker-compose.yml](docker-compose.yml).
 
 We here detail the main parameters that should or can be modified in an environment file. 

### Paths

You should set the variable `repo_path` to the output of the command 
`$(git rev-parse --show-toplevel)`. 

You should NOT edit the `indocker_repo_path` variable, but if you do that would require rebuilding
the docker image (see folder [DockerBuild](../../DockerBuild)).

### HBase
You should edit the HBase related parameters in your environment file, to make sure that:

- The HBase host `hbase_host` is correct.
- The HBase tables `table_sha1infos` and `table_updateinfos` names are correct.
Note that these tables would be created by the pipeline if they do not exist yet.

The other HBase parameters `batch_update_size`, `extr_column_family`, `image_info_column_family`,
`image_buffer_column_family` and `image_buffer_column_name` could be kept as is but should be
consistent with the values used in the processing component. 

### Searcher

Search parameters:
- `search_conf_name`: name of the search configuration, will influence the name of the index saved
as well as the codes files.
- `nb_train`: number of training samples for the indexing model
- `nb_min_train`: minimum number of training samples for the indexing model
- `nb_train_pca`: number of training samples for the PCA
- `nb_min_train_pca`: minimum number of training samples for the PCA

LOPQ parameters, see [LOPQ](../../../lopq) for more details:
- `model_type`: currently `lopq_pca` is the only indexing model fully supported
- `lopq_pcadims`: number of dimensions of the PCA
- `lopq_V`: number of clusters per coarse quantizer (bigger value induce lower quantization error)
- `lopq_M`: total number of fine codes (bigger value induce lower quantization error)
- `lopq_subq`: number of clusters to train per subquantizer (256 is a good value)

### Storer
A storer is used to save the trained index model and the computed codes.

If you use the `S3` storer, you should edit the AWS related parameters:

- You should edit the parameters `aws_profile` and `aws_bucket_name` in the environment file so it correspond to 
the actual profile and bucket you want to use. 
- You should copy the [sample AWS credentials file](../../../conf/aws_credentials/credentials.sample) to 
`conf/aws_credentials/credentials` and edit it to contain the information corresponding
to the target profile `aws_profile`.

### API

The API parameters configure at which URL the service could be access, gunicorn parameters and 
the expected input type:
* `port_host`: port the service should use
* `endpoint`: name of the service, would change the API URL
* `gunicorn_workers`: number of workers for gunicorn
* `gunicorn_timeout`: startup timeout of gunicorn
* `search_input`: query type, `face` or `image`

### Others

The extraction type variable `extr_type` defines what features are indexed and should be extracted
from the query, this should be currently either `sbpycaffeimg` or `dlibface`.

There is a general `verbose` parameter that would be propagated to most of the python classes and
controls the level of logging. Increase this value to perform debugging, reduce it to lower the 
output flow. 

The input type variable `input_type` specifies whether the images were pushed from a local source
 and then it should be set to `local` or from any other source and should be to something else 
 e.g. `kafka` or `kinesis`. In this componenet it is mostly used to properly display images
 when calling the endpoints `view_similar_byX` like `view_similar_byURL` of the API. 

TODO: Check the additional parameter `file_input` and interaction with `input_type`.

## Image search service

### Search Index
The service will first train a search index if one corresponding to the settings of the configuration file is not available
in the storer, which could be either `local` or `S3`.

The service will look for updates listed in the HBase table `table_updateinfos` and accumulate
the corresponding image features up to `nb_train_pca` and will first train a PCA model. 
Then it will accumulate `nb_train` PCA projected features to train the search index. 
These features are stored locally in a LMDB database to potentially allow easy retraining with 
different settings.
If not enough features are yet available in this database, the training process will wait for 
more features to be computed by the processing pipeline. 
If you have trained your index model and no longer plan to train another index you should delete
 the training features LMDB database.

Once the model is trained it is saved to the storer. 
For a `S3` storer it will be using the AWS profile `aws_profile` and the bucket `bucket_name`.
For a `local` storer it will save in the folder `base_path`.

### Indexing Updates

Then all the updates listed in the HBase table `HBI_table_updateinfos` are indexed, i.e. the 
LOPQ codes of each image feature is computed and stored in a local LMDB database, 
and also pushed to the storer as a backup. If you are streaming data into the processing pipeline
you should also query from time to time the update endpoints as listed below.

### API

Once all images are indexed the REST API is started.
The name of the API service is set by the parameter `endpoint` and it listens to the 
port `port_host` defined in the environment file. 
Thus, the API would be accessible at `localhost:80/cuimgsearch` if `endpoint` is set 
to `cuimgsearch` and `port_host` to 80.
 
 More details on how to use the API is provided in the [www](../../../www) folder and you can 
 check the code of the API in the [API](../../../cufacesearch/cufacesearch/api) folder.  


## Docker-compose

The docker-compose file [docker-compose.yml](docker-compose.yml) in this directory should be used 
to start an image search component based on the configuration defined in the `.env` file.

Copy (and edit) one of the example environment file `.env_test_sb`, `.env_release_sb` 
or `.env_merged_sb` to `.env`.

### Start the docker container
Just run to start the docker container:

`(sudo) docker-compose up`

Once everything is running you can detach the docker-compose process by pressing `Ctrl + z`. 
You can check the docker logs with the command:

`(sudo) docker logs -f [docker-container-name]`

### Check the docker container
The `docker-container-name` should be `search_img_search_1` and you can check it with `(sudo) docker ps`.

### Update the index

To check and keep the API up-to-date you need to call the endpoints:
* `status`: return a JSON with information about uptime and number of samples indexed
* `check_new_updates`: would check updates starting from the last update indexed
* `check_all_updates`: would check updates from the beginning of time

