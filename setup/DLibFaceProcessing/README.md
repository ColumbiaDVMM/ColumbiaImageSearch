# Face processing setup

## Start script

The script [start_docker_dlib_face_processing.sh](start_docker_dlib_face_processing.sh) will build the docker image from the docker file, 
setup all dependencies (using the script [setup_dlib_face_processing.sh](setup_dlib_face_processing.sh)) and start the processing script.  
You can later on use this script to restart the processing, it will not rebuild the docker image if it is already there.


This processing pipeline is divided into two processes, extraction checker and extraction processor.
Based on the values in the configuration file, these two processes will perform the following tasks:
 
- extraction checker: that reads images form the Kafka topic `KIcheck_consumer_topics`,
check that they have not yet been marked as processed in the HBase table `HBI_table_sha1infos`
and creates batches of `HBI_batch_update_size` unprocessed images and pushed them to both 
the HBase table `HBI_table_updateinfos` and the Kafka topic `KIcheck_producer_updates_out_topic`.
- extraction processor: that reads from the Kafka topic `KIproc_consumer_topics` (that should be the same as 
`KIcheck_producer_updates_out_topic`) run the face detection and compute the face features and push them 
(encoded in Base64) to the HBase table `HBI_table_sha1infos`. 

Once the docker is started and the extraction checker and processor are running you should see
two log files with filenames starting with `log_check` and `log_proc` appear in this folder.
If the docker is running but this log files are not created, it should mean you 
did not properly configure some paths in the scripts as detailed below. 
You should also check these log files for any error.  

### Script parameters

You should edit the following parameters:

- repo path (or just base_path) in the script [start_docker_dlib_face_processing.sh](start_docker_dlib_face_processing.sh): absolute path to the root of this repository. 
- suffix in the script [run_dlib_face_processing.sh](run_dlib_face_processing.sh): corresponding to whatever suffix (e.g. `test`) you use for your configuration file.  

## Configuration file

You should edit the Kafka related parameters in your config file (e.g. [conf_extr_dlibface_test.json](../../conf/conf_extr_dlibface_test.json)), 
to make sure that:

- the servers lists `KIcheck_consumer_servers`, `KIcheck_producer_servers` and 
`KIproc_consumer_servers` are pointing to the correct addresses 
of your Kafka brokers.
- the certificates listed in 
`KIcheck_consumer_security`, `KIcheck_producer_security` and `KIproc_consumer_security`
to connect to the Kafka broker are available at the location 
defined in the config file. 
The relative path is with regards to the folder containing this README.md.


You should edit the HBase related parameters in your config file, to make sure that:

- The HBase host `HBI_host` is correct.
- The HBase tables `HBI_table_sha1infos` and `HBI_table_updateinfos`.
Note that these tables would be created by the pipeline if they do not exist yet.

You may lower the parameter `EXTR_nb_threads` to decrease the CPU and memory usage. 
You may decrease/increase the parameter `KIcheck_verbose` and `KIproc_verbose` in the range [0,6] to get less/additional information in 
the log file.

The model files `DLIB_pred_path` and `DLIB_rec_path` will be downloaded automatically. 
There is no need to change these values.
