# Setup

This folder contains the docker-compose files and some setup scripts to ease the setup process of the system.

## All-in-one setup

The folder [all-in-one](./all-in-one) contains two examples of image index
you can build from publicly available datasets just by running a single command. 
This is intended to be used as a testbed and to get you familiar with the tool. 
Check the [README.md](./all-in-one/README.md) in that folder for more details.

## Components setup

In a production environment, each component (ingestion, processing, search) would be better run on separate machines
and relying real cluster instances of Kafka and HBase.
The folders in [components](./components) provide docker-compose files and sample environments files
for that purpose.
You might need to adjust some parameters (number of workers, number of threads etc.) to the hardware you use,
to optimize the computation or avoid overloading the machines you are using. 
Default `release` parameters are somewhat optimized to the expected hardware and computation load required 
for each component for the MEMEX transition.

More details are available in each sub-folder's README.md file. 

## Configuration generation

The configuration generation scripts in [ConfGenerator](./ConfGenerator) are called automatically when starting the docker containers with
the provided docker-compose files.

## Docker build

The folder [DockerBuild](./DockerBuild) contains the script and Docker file that were used to build the image that was pushed to the
docker repository and that is pulled when starting up the docker containers using the provided docker-compose files.