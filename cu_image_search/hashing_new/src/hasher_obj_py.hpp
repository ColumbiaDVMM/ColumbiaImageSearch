#ifndef HASHEROBJ_PY
#define HASHEROBJ_PY

#include "hasher_obj.hpp"


// Simpler HasherObject to be wrapped with SWIG
class HasherObjectPy {

    public:

        HasherObjectPy() {
            hobj = new HasherObject();
        };

        ~HasherObjectPy() {
            delete hobj;
        };

        int initialize() {
            return hobj->initialize();
        };

        int read_update_files() {
            return hobj->read_update_files();
        };

        int load_hashcodes() {
            hobj->fill_data_nums_accum();
            return hobj->load_hashcodes();
        };

        int load_itq_model() {
            return hobj->load_itq_model();
        };

        void set_query_feats_from_disk(std::string filename) {
            hobj->set_query_feats_from_disk(filename);
        };

        void find_knn() {
            hobj->find_knn();
        };

        // Not yet working.
        vector< vector< pair<float, int> > > find_knn_nodiskout() {
            return hobj->find_knn_nodiskout();
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

        void set_near_dup_th(float _near_dup_th) {
            hobj->set_near_dup_th(_near_dup_th);
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

        void set_base_modelpath(std::string _base_modelpath){
            hobj->set_base_modelpath(_base_modelpath);
            hobj->set_paths();
        };

        std::string get_base_modelpath(){
            return hobj->get_base_modelpath();
        };

        void set_base_updatepath(std::string _base_updatepath) {
            hobj->set_base_updatepath(_base_updatepath);
            hobj->set_paths();
        };

        std::string get_base_updatepath() {
            return hobj->get_base_updatepath();
        };

        void set_outputfile(std::string _outname){
            hobj->set_outputfile(_outname);
        };

    private:

        HasherObject* hobj;

};


#endif
