%module hasher_obj_py

//%include "typemaps.i"
//%include "stl.i"

%include "std_string.i"
%include "std_vector.i"
%include "std_pair.i"

%{
 #define SWIG_PYTHON_EXTRA_NATIVE_CONTAINERS 
 /* Includes the header in the wrapper code */
 #include "hasher_obj_py.hpp"
%}

namespace std
{
  %template(PairF) pair<float,int>;
  %template(InnerResVector) vector< pair<float,int> >;
  %template(ResVector) vector< vector< pair<float,int> > >;
}


// Tell swig to put type information into the functions docstrings... 
%feature("autodoc", "1");

/* Parse the header file to generate wrappers */
%include "hasher_obj_py.hpp"
