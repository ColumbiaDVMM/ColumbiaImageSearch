#
# Ubuntu Dockerfile
#
# https://github.com/dockerfile/ubuntu
#


# Pull base image.
FROM ubuntu:trusty

# Setup and configure
ENV DEBIAN_FRONTEND noninteractive
RUN \
  apt-get update && \
  apt-get install -y --no-install-recommends apt-utils software-properties-common --force-yes && \
  add-apt-repository ppa:fkrull/deadsnakes-python2.7 && \
  apt-get update && \
  apt-get -y upgrade --force-yes && \
  apt-get install locales && \
  locale-gen en_US.UTF-8 && \
  apt-get install -y build-essential --no-install-recommends --force-yes && \
  apt-get install -y curl git python-dev libpng-dev libjpeg8-dev libfreetype6-dev pkg-config libblas-dev liblapack-dev libatlas-base-dev gfortran cmake libboost-all-dev --no-install-recommends --force-yes && \
  apt-get install -y nano less screen openssh-client --force-yes && \
  curl --silent --show-error --retry 5 https://bootstrap.pypa.io/get-pip.py | sudo python


# Create needed directories
RUN mkdir /home/ubuntu && mkdir /home/ubuntu/memex && mkdir /home/ubuntu/memex/update && \
    mkdir /home/ubuntu/memex/ColumbiaFaceSearch

# One line ENV setting
ENV LANG=en_US.UTF-8 LANGUAGE=en_US:en LC_ALL=en_US.UTF-8 HOME=/home/ubuntu

# Define default command.
CMD ["bash"]
