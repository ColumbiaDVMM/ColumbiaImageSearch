%module hasher_obj_py

%include <std_string.i>
%include "typemaps.i"
%include "stl.i"

%{
 /* Includes the header in the wrapper code */
 #include "hasher_obj_py.hpp"
%}

namespace std
{
  %template(PairF) std::pair<float,int>;
  %template(InnerResVector) std::vector< std::pair<float,int> >;
  %template(ResVector) std::vector< std::vector< std::pair<float,int> > >;
}


// Tell swig to put type information into the functions docstrings... 
%feature("autodoc", "1");

/* Parse the header file to generate wrappers */
%include "hasher_obj_py.hpp"
