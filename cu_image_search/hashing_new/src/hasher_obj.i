%module hasher_obj_py

%include <std_string.i>
%include "typemaps.i"
%include "stl.i"

%{
 #define SWIG_PYTHON_EXTRA_NATIVE_CONTAINERS 
 /* Includes the header in the wrapper code */
 #include "hasher_obj_py.hpp"
 %typedef std::pair<float,int> mypairf;
%}

namespace std
{
  %template(ResMyPairF) mypairf;
  %template(InnerResVector) vector< mypairf >;
  %template(ResVector) vector< vector< mypairf > >;
}


// Tell swig to put type information into the functions docstrings... 
%feature("autodoc", "1");

/* Parse the header file to generate wrappers */
%include "hasher_obj_py.hpp"
