%module hasher_obj_py

%include <std_string.i>
%include "typemaps.i"
%include "stl.i"

%{
 /* Includes the header in the wrapper code */
 #include "hasher_obj_py.hpp"
%}

/*
class HasherObjectPy {

    public:

        HasherObjectPy();
        ~HasherObjectPy();

        int read_update_files();
        int load_hashcodes();
        int load_itq_model();

        void set_query_feats_from_disk(std::string filename);
        void find_knn();
        void set_paths();
        void set_topk(int _top_k);
        void set_ratio(float _ratio);
        void set_bit_num(int _bit_num);
        void set_norm(int _norm);
        void set_feature_dim(int _feature_dim);
        void set_base_modelpath(std::string _base_modelpath);
        std::string get_base_modelpath();
        void set_base_updatepath(std::string _base_updatepath);
        std::string get_base_updatepath();
        void set_outputfile(std::string _outname);
};*/


// Tell swig to put type information into the functions docstrings... 
%feature("autodoc", "1");

/* Parse the header file to generate wrappers */
%include "hasher_obj_py.hpp"
