//swig -python -c++ -o src/hasher_obj_wrap.cxx src/hasher_obj.i
//g++ -O2 -fPIC -c src/hasher_obj_wrap.cxx -I/opt/local/include/ -I./src/ -I/usr/include/python2.7/ -o obj/hasher_obj_wrap.o
//g++ -shared obj/hasher_obj.o obj/hasher_obj_wrap.o obj/header.o obj/iotools.o -L/opt/local/lib/ -lopencv_core -lopencv_highgui -lz -lpython2.7 -o _hasher_obj_py.so


#ifndef HASHEROBJ_PY
#define HASHEROBJ_PY

#include "hasher_obj.hpp"

// // This needs to be in any "main"
// string base_modelpath;
// string base_updatepath;
// string update_files_listname;
// string update_hash_folder;
// string update_feature_folder;
// string update_compfeature_folder;
// string update_compidx_folder;
// string update_files_list;
// string update_hash_prefix;
// string update_feature_prefix;
// string update_compfeature_prefix;
// string update_compidx_prefix;

// Simpler HasherObject to be wrapped with SWIG
class HasherObjectPy {
    
    public:
        
        HasherObjectPy() {
            // set default values
            hobj = new HasherObject();
        };

        ~HasherObjectPy() {
            delete hobj;
        };

        int read_update_files() {
            return hobj->read_update_files();
        };

        int load_hashcodes() {
            return hobj->load_hashcodes();
        };

        int load_itq_model() {
            return hobj->load_itq_model();
        };
        
        void set_query_feats_from_disk(string filename) {
            hobj->set_query_feats_from_disk(filename);
        };
        
        void find_knn() {
            hobj->find_knn();
        };

        void set_paths() {
            hobj->set_paths();
        };

        void set_topk(int _top_k) {
            hobj->set_topk(_top_k);
        };

        void set_ratio(float _ratio) {
            hobj->set_ratio(_ratio);
        };

        void set_bit_num(int _bit_num) {
            hobj->set_bit_num(_bit_num);
        };

        void set_norm(int _norm) {
            hobj->set_norm(_norm);
        };

        void set_feature_dim(int _feature_dim) {
            hobj->set_feature_dim(_feature_dim);
        };

        void set_base_modelpath(string _base_modelpath){
            hobj->set_base_modelpath(_base_modelpath);
        };

        void set_base_updatepath(string _base_updatepath) {
            hobj->set_base_updatepath(_base_updatepath);
        };

        void set_outputfile(string _outname){
            hobj->set_outputfile(_outname);
        };


    private:
        
        HasherObject* hobj;

};


#endif