# Face search

This repository implements a face search tool developed for the [MEMEX project](https://www.darpa.mil/program/memex).

The face detection and recognition model are currently the publicly available models from the [DLib](http://blog.dlib.net/) library, 
see the blog post [DLib face recognition](http://blog.dlib.net/2017/02/high-quality-face-recognition-with-deep.html) 
for more information about the models. However, the package 'cufacesearch' has been written in a modular way and using 
another face detection or recognition model should be fairly easy.

Author: [Svebor Karaman](mailto:svebor.karaman@columbia.edu)

## Installation 

### Docker installation

For convenience a docker installation process is provided in [setup](./setup), 
check the README.md in that folder for additional information.

### Manual installation

#### Dependecies

If you want to install the tool without docker, you should first install the packages needed. 
For Ubuntu:

- sudo apt-get install git python-pip python-dev libpng-dev libjpeg8-dev libfreetype6-dev pkg-config libblas-dev liblapack-dev libatlas-base-dev gfortran cmake libboost-all-dev

#### Setup python packages 

Then running the script [setup_face_search.sh](./setup/setup_face_search.sh) with the parameter '-r' set to the absolute 
path of this repo should be enough.

#### Manual execution

Run the script [keep_alive_face_api.sh](./www/keep_alive_face_api.sh).

Edit the following parameters to match your installation:

- CONF_FILE
- API_FOLDER
- LOG_FOLDER

## Configuration

Most of the settings can be configured through a JSON file passed as parameter to the API.
Some examples configuration files are provided in the [./conf](conf) folder.




