%module hasher_obj_py

%include <std_string.i>
%include "typemaps.i"
%include "stl.i"

namespace std
{
  %template(ResPair) pair<float, int>;
  %template(InnerResVector) vector< pair<float, int> >;
  %template(ResVector) vector< vector< pair<float, int> > >;
}

%{
 /* Includes the header in the wrapper code */
 #include "hasher_obj_py.hpp"
%}

// Tell swig to put type information into the functions docstrings... 
%feature("autodoc", "1");

/* Parse the header file to generate wrappers */
%include "hasher_obj_py.hpp"
