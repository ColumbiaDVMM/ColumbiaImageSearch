#!/bin/bash
PY_CFLAGS=$(python-config --cflags)
PY_LDFLAGS=$(python-config --ldflags)
OPENMP=-fopenmp

swig -python -c++ -o ../src/hasher_obj_wrap.cxx ../src/hasher_obj.i
g++ -O2 -fPIC -c ../src/hasher_obj_wrap.cxx  ${PY_CFLAGS} -I/opt/local/include/ -I../src/ -o ../obj/hasher_obj_wrap.o
g++ -shared ../obj/hasher_obj_fpic.o ../obj/path_manager_fpic.o ../obj/hasher_obj_wrap.o ../obj/header_fpic.o ../obj/iotools_fpic.o  ${PY_LDFLAGS} -L/opt/local/lib/ -lopencv_core -lopencv_highgui -lz ${OPENMP} -o _hasher_obj_py.so

# so that HasherSwig import locally
cp _hasher_obj_py.so ../../hasher/
