# Docker installation

## Start script

The script [start_docker_face_search.sh](start_docker_face_search.sh) will build the docker image from the docker file, 
setup all dependencies (using the script [setup_face_search.sh](setup_face_search.sh)) and start the face search API.  
You can later on use this script to restart the face search API.

You should edit the following parameters in the script:
- repo_path: absolute path to the root of this repository. 
- data_path: absolute path to the folder containing the face data.
- PORT: the port on which the API is listening (default: 5000)

For now, this script assumes you have some precomputed face data in the 'data_path' save in text files with the format
accept by the function 'parse_feat_line' in [cufacesearch/featurizer/featsio.py](../cufacesearch/cufacesearch/featurizer/featsio.py), that is:
- sha1\ts3_url\t./local_path_to_be_ignored.jpg\tface_top\tface_bottom\tface_left\tface_right\tfeat_val1\t...\n