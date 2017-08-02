# Setup an image similarity search service

The script start_docker_columbia_image_search.sh in this directory will build the docker image with all needed dependencies.

You have to specify 4 parameters to this script:
- -c should be the path from the root of the repository to the config file you want to use, e.g conf/global_var_sample_local.json
- -n is the name of the domain you are indexing, it will be used as suffix for naming the docker and indexing folder
- -p the port on which you want to access the image search service on the host machine
- -d should be the full path of the directory containing local images to be indexed [if the config file is setup to use local indexing]

./start_docker_columbia_image_search.sh -d /Users/svebor/Documents/Workspace/CodeColumbia/Datasets/Caltech101 -c 
conf/global_var_sample_local.json -n caltech101 -p 80
