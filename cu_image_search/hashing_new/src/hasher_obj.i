%module hasher_obj_py


%{
 /* Includes the header in the wrapper code */
 #include "hasher_obj_py.hpp"
%}

%include "typemaps.i"
%include "stl.i"


// Tell swig to put type information into the functions docstrings... 
%feature("autodoc", "1");

/* Parse the header file to generate wrappers */
%include "hasher_obj_py.hpp"
