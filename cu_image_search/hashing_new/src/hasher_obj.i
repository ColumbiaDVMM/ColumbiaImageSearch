%module hasher_obj_py

%{
 /* Includes the header in the wrapper code */
 #include "hasher_obj_py.hpp"
%}

%include "typemaps.i"
%include "stl.i"

class HasherObjectPy {
    
    public:
        
        HasherObjectPy();
        ~HasherObjectPy();

        int read_update_files();
        int load_hashcodes();
        int load_itq_model();
        
        void set_query_feats_from_disk(string filename);
        void find_knn();
        void set_paths();
        void set_topk(int _top_k);
        void set_ratio(float _ratio);
        void set_bit_num(int _bit_num);
        void set_norm(int _norm);
        void set_feature_dim(int _feature_dim);
        void set_base_modelpath(string _base_modelpath);
        void set_base_updatepath(string _base_updatepath);
        void set_outputfile(string _outname);
};


// Tell swig to put type information into the functions docstrings... 
%feature("autodoc", "1");

/* Parse the header file to generate wrappers */
//%include "hasher_obj_py.hpp"
