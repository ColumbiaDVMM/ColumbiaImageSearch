%module hasher_obj_py

%include <std_string.i>
%include "typemaps.i"
%include "stl.i"

%{
 /* Includes the header in the wrapper code */
 #include "hasher_obj_py.hpp"
%}

// Tell swig to put type information into the functions docstrings... 
%feature("autodoc", "1");

/* Parse the header file to generate wrappers */
%include "hasher_obj_py.hpp"
