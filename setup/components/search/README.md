## Image search service

[TODO: old text, to be rewritten to better describe current docker-compose, environment file settings (work in progress)]

The service will first train a search index if one corresponding to the settings of the configuration file is not available
in the storer, either `local` or `S3`.
The service will look for updates listed in the HBase table `HBI_table_updateinfos` and accumulate
the corresponding image features up to `SEARCHLOPQ_nb_train_pca` and will first train a PCA model. 
Then it will accumulate `SEARCHLOPQ_nb_train` PCA projected features to train the search index. 
These features are stored locally in a LMDB database to potentially allow easy retraining with different settings.
Note that if not enough features are available, the training process will wait for more features to be computed
by the processing pipeline. 

Once the model is trained it is saved to the storer. 
For a `S3` storer it will be using the AWS profile `ST_aws_profile` and the bucket `ST_bucket_name`.
For a `local` storer it will save in the folder `ST_base_path`.
Then all the updates listed in the HBase table `HBI_table_updateinfos` are indexed, i.e. the LOPQ codes of each image 
feature is computed and stored in a local LMDB database, and also pushed to the storer as a backup.

Once all images are indexed the REST API is started and listen to the port `PORT_HOST`  (default `80`) defined in the start script. 
The name of the endpoint is set in the run script as the parameter `endpoint` (default `cuimgsearch`).
Thus, by default, the API would be accessible at `localhost:80/cuimgsearch`, more details in the API section below.  

## Environment file settings

### Kafka

Check that the kafka servers, security parameters and topics names are as they should.

### HBase
You should edit the HBase related parameters in your environment file, to make sure that:

- The HBase host `hbase_host` is correct.
- The HBase tables `table_sha1infos` and `table_updateinfos`.
Note that these tables would be created by the pipeline if they do not exist yet.

### Storer
A storer is used to save the trained index model and the computed codes.

If you use the `S3` storer, you should edit the AWS related parameters:

- You should edit the parameters `aws_profile` and `aws_bucket_name` in the environment file so it correspond to 
the actual profile and bucket you want to use. 
- You should copy the [sample AWS credentials file](../../conf/aws_credentials/credentials.sample) to 
`conf/aws_credentials/credentials` and edit it to contain the information corresponding
to the profile `aws_profile`.
