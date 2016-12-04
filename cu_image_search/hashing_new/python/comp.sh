#!/bin/bash
PY_CFLAGS=$(python-config --cflags)
PY_LDFLAGS=$(python-config --ldflags)

swig -python -c++ -o ../src/hasher_obj_wrap.cxx ../src/hasher_obj.i
g++ -O2 -fPIC -c ../src/hasher_obj_wrap.cxx  ${PY_CFLAGS} -I/opt/local/include/ -I../src/ -o ../obj/hasher_obj_wrap.o
g++ -shared ../obj/hasher_obj_fpic.o ../obj/path_manager_fpic.o ../obj/hasher_obj_wrap.o ../obj/header_fpic.o ../obj/iotools_fpic.o  ${PY_LDFLAGS} -L/opt/local/lib/ -lopencv_core -lopencv_highgui -lz -o _hasher_obj_py.so


#g++ -O2 -fPIC -c ../src/hasher_obj_wrap.cxx -I/opt/local/include/ -I../src/ -I/Users/svebor/anaconda/include/python2.7/ -o ../obj/hasher_obj_wrap.o

#g++ -shared ../obj/hasher_obj.o ../obj/hasher_obj_wrap.o ../obj/header.o ../obj/iotools.o -L/opt/local/lib/ -lopencv_core -lopencv_highgui -lz -L/Users/svebor/anaconda/lib/ -lpython2.7 -o _hasher_obj_py.so
#g++ -shared ../obj/hasher_obj.o ../obj/hasher_obj_wrap.o ../obj/header.o ../obj/iotools.o -L/opt/local/lib/ -lopencv_core -lopencv_highgui -lz -l/Users/svebor/anaconda/lib/libpython2.7.dylib -o _hasher_obj_py.so
#g++ -bundle -shared ../obj/hasher_obj.o ../obj/hasher_obj_wrap.o ../obj/header.o ../obj/iotools.o -L/opt/local/lib/ -lopencv_core -lopencv_highgui -lz -l/Users/svebor/anaconda/lib/libpython2.7.dylib -o _hasher_obj_py.so

# issue with dynamic libraries and version of python
# linking is done with python from /System/Library/Frameworks/Python.framework/Versions/2.7/bin/python
# http://stackoverflow.com/questions/33281753/unsafe-use-of-relative-rpath-libboost-dylib-when-making-boost-python-helloword-d

#install_name_tool -change libopencv_highgui.2.4.dylib /usr/local/lib/libopencv_highgui.2.4.dylib _hasher_obj_py.so 
#install_name_tool -change libopencv_core.2.4.dylib /usr/local/lib/libopencv_core.2.4.dylib _hasher_obj_py.so 
# issue propagates down to other opencv libraries...
