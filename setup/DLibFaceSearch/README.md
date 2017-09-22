# Docker installation

## Start script

The script [start_docker_dlib_face_search.sh](start_docker_dlib_face_search.sh) will build the docker image from the docker file, 
setup all dependencies (using the script [setup_dlib_face_search.sh](setup_dlib_face_search.sh)) and start the processing script.  

You can later on use this script to restart the processing, it will not rebuild the docker image if it is already there.

You should edit the following parameters:

- repo path (or just base_path) in the script [start_docker_dlib_face_search.sh](start_docker_dlib_face_search.sh): 
absolute path to the root of this repository. 
- suffix in the script [run_dlib_face_search.sh](run_dlib_face_search.sh): 
wether you are running in test (using first a subset of data to check setup is correct is recommended) or release mode.
- nb_threads in the config file ([conf_search_dlibface_test.json](../../conf/conf_search_dlibface_test.json) or [conf_search_dlibface_release.json](../../conf/conf_extr_dlibface_release.json)) to the number of cpus available for the image processing. 

If you are using the `s3` storer in the config file, you need to have 
the corresponding `AWS profile` in your `~/.aws/credentials` file. 
