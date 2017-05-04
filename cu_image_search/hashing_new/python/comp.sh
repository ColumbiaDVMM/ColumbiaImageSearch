#!/bin/bash

#-- conf
BASEDIR=/usr/local
CC=g++
OPENMP=-fopenmp
# local
#BASEDIR=/opt/local
#CC=g++-mp-4.9
#OPENMP=
#-- end conf

CXXFLAGS="-std=c++0x -O2"
PY_CFLAGS=$(python-config --cflags)
PY_LDFLAGS=$(python-config --ldflags)

LIB_DIRS=${BASEDIR}/lib/
INCLUDE_DIRS="${BASEDIR}/include/ ../src/"
INCLUDE_DIRS_CMD=$(for d in ${INCLUDE_DIRS}; do echo " -I"${d}; done)
echo "Will use include dirs: "${INCLUDE_DIRS_CMD}

# compile
swig -python -c++ -o ../src/hasher_obj_wrap.cxx ../src/hasher_obj.i
${CC} ${CXXFLAGS} -fPIC -c ../src/hasher_obj_wrap.cxx  ${PY_CFLAGS} ${INCLUDE_DIRS_CMD} -o ../obj/hasher_obj_wrap.o
${CC} ${CXXFLAGS} -shared ../obj/hasher_obj_fpic.o ../obj/path_manager_fpic.o ../obj/hasher_obj_wrap.o ../obj/header_fpic.o ../obj/iotools_fpic.o  ${PY_LDFLAGS} -L${LIB_DIRS} -lopencv_core -lopencv_highgui -lz ${OPENMP} -o _hasher_obj_py.so

# export, so that HasherSwig import locally
cp _hasher_obj_py.so ../../hasher/
