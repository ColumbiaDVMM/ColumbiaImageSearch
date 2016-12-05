#ifndef HASHEROBJ
#define HASHEROBJ

#include "header.h"
#include "iotools.h"
//#include "path_manager.hpp"
#include <stdio.h>
#include <opencv2/opencv.hpp>
#include <fstream>

using namespace std;
using namespace cv;


class HasherObject {
    
    public:
        // What should the parameters be here?
        HasherObject() {
            // set default values
            feature_dim = 4096;
            bit_num = 256;
            int_num = bit_num/32;
            // l2-norm features
            norm = true;
            // number of features to retrieve for reranking
            // would be overwritten based on ratio and data_num
            top_feature = 2000;
            ratio = 0.001f;
            // number of features indexed
            data_num = 0;
            // initialize path manager
            pm.set_paths(norm, bit_num);
            // initialize timing
            t[0] = 0.0;
            reset_timings();
            // initialize output buffer to speed up writing out results
            outputfile.rdbuf()->pubsetbuf(buffer, length);
        };

        // What need to be freed/closed?
        ~HasherObject() {
            itq.release();
            W.release();
            mvec.release();
            top_feature_mat.release();
            postrank.clear();
            hamming.clear();
            // accum?
            // query_codes?
        };

        void reset_timings() {
            for (int _t=1; _t<8; _t++)
                t[_t] = 0.0; 
        }

        int initialize() {
            double t_start = get_wall_time();
            int status = read_update_files();
            if (status != 0)
                return status;
            fill_data_nums_accum();
            status = load_itq_model();
            if (status != 0)
                return status;
            status = load_hashcodes();
            if (status != 0)
                return status;
            double t_init = get_wall_time() - t_start;
            cout << "[initialize] Done in " << t_init << "s." << endl;
            return 0;
        };

        int read_update_files();


        // Load DB hashcodes from files listed in update_file
        int load_hashcodes();

        // Load ITQ model
        int load_itq_model();

        // io from disk
        Mat read_feats_from_disk(string filename);

        //Mat read_hashcodes_from_disk(string filename);
        
        void set_query_feats_from_disk(string filename);

        // compute hashcodes from feats
        unsigned int* compute_hashcodes_from_feats(Mat feats);

        // query methods
        // use member query_feats, assumes set_query_feats_from_disk have been called before
        void find_knn();

        void find_knn_from_feats(Mat query_feats);

        //Mat find_knn_from_hashcodes(Mat query_hashcodes);


        void set_paths();

        // To force top_feature. 
        // To be called after fill_data_nums_accum
        void set_topk(int _top_k) {
            top_feature = _top_k;
        };

        void set_ratio(float _ratio) {
            ratio = _ratio;
            set_top_feature();
        };

        void set_bit_num(int _bit_num) {
            bit_num = _bit_num;
            int_num = bit_num/32;
        };

        void set_norm(int _norm) {
            norm = _norm;
        };

        void set_feature_dim(int _feature_dim) {
            feature_dim = _feature_dim;
        };

        void set_base_modelpath(string _base_modelpath){
            pm.base_modelpath = _base_modelpath;
        };

        string get_base_modelpath() {
            return pm.base_modelpath;
        };

        void set_base_updatepath(string _base_updatepath) {
            pm.base_updatepath  = _base_updatepath;
        };

        string get_base_updatepath() {
            return pm.base_updatepath;
        };

        void set_outputfile(string _outname){
            outname = _outname;
        };

        void init_feature_mat() {
            top_feature_mat.create(top_feature, feature_dim, CV_32F);
        };

        void fill_data_nums_accum();
        void clean_compfeat_files();

        // // io from memory
        // Need to use boost::python converter for cv::Mat?
        // maybe later...
        // Mat get_feats_from_memory(void* data);

        // Mat get_hashcodes_from_memory(void* data);


    private:
        // parameters values
        float ratio;
        int feature_dim;
        int int_num;
        // only these two parameters influence filenames
        int bit_num;
        int norm;

        // number of features to retrieve for reranking
        int top_feature;
        // number of features indexed
        unsigned long long int data_num;
        // number of samples in each update files
        int* accum;

        // Hashing related things
        // contains the hashcodes
        Mat itq;
        // contains the projections vectors
        Mat W;
        // contains the mean vectors
        Mat mvec;

        // Initializing features structures and files streams
        Mat top_feature_mat;
        
        // timing
        double t[8];

        // To manage paths/strings
        PathManager pm; 

        // List of data files
        vector<string> update_hash_files;
        vector<string> update_compfeature_files;
        vector<string> update_compidx_files;
        // Compressed features index and data
        vector<ifstream*> read_in_compidx;
        vector<ifstream*> read_in_compfeatures;
        // List of number of features per update file
        vector<unsigned long long int> data_nums;

        // Internal structure to store query features and hashcodes
        Mat query_feats; 
        unsigned int* query_codes; 
        vector<mypair> hamming;
        vector<mypairf> postrank;
        int query_num;

        // Output streams
        string outname;
        ofstream outputfile;
        ofstream outputfile_hamming;

        // Use a single ifstream and read_size to read things
        ifstream read_in;
        size_t read_size;

        void set_top_feature() {
            top_feature = (int)ceil(data_num*ratio);
            cout << "Will retrieve the top " << top_feature << " features." << endl;
            init_top_features_mat();
        };

        void init_top_features_mat() {
            top_feature_mat.release();
            top_feature_mat.create(top_feature, feature_dim, CV_32F);
            postrank.clear();
            postrank.reserve(top_feature);
        };

        // To be used to speed up writing out results
        static const unsigned int length = 8192;
        char buffer[length];

        void init_output_files();
        void close_output_files();
        void init_output_files(string outname);
        void write_to_output_file(vector<mypairf> postrank, vector<mypair> hamming);
        vector<mypair> compute_hamming_dist_onehash(unsigned int* query);
        vector<mypairf> rerank_knn_onesample(float* query_feature, vector<mypair> top_hamming);
};


#endif
