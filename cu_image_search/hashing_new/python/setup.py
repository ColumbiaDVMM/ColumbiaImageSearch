# actually not used. compiled with comp.sh
# setup.py

from distutils.core import setup, Extension

hasher_obj_py = Extension('_hasher_obj_py', sources=['hasher_obj_py.cpp', 'hasher_obj_py.i'])

setup(name='hasher_obj_py', ext_modules=[hasher_obj_py], py_modules=["hasher_obj_py"])
