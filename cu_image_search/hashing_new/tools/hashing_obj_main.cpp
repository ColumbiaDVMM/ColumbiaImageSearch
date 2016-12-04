#include "hasher_obj.hpp"

#include <stdio.h>
#include <opencv2/opencv.hpp>
#include <fstream>

using namespace std;
using namespace cv;

// Additional output of hamming distances if DEMO==0
#define DEMO 1

int main(int argc, char** argv){

    double t[2]; // timing
    t[0] = get_wall_time(); // Start Time
    float runtimes[7] = {0.0f,0.0f,0.0f,0.0f,0.0f,0.0f,0.0f};
    
    if (argc < 2){
        cout << "Usage: hashing_obj feature_file_name [base_modelpath base_updatepath hashing_bits post_ranking_ratio normalize_features read_threshold]" << std::endl;
        return -1;
    }
    
    // Initialize hashobject
    HasherObject* hasher_obj = new HasherObject();

    // Set properties
    if (argc>2)
        hasher_obj->set_base_modelpath(string(argv[2]));
    if (argc>3)
        hasher_obj->set_base_updatepath(string(argv[3]));
    if (argc>4)
        hasher_obj->set_bit_num(atoi(argv[4]));
    if (argc>5)
        hasher_obj->set_ratio((float)atof(argv[5]));
    if (argc>6)
        hasher_obj->set_norm(atoi(argv[6]));
    hasher_obj->set_paths();

    // Load DB hashcodes and model
    int status_ok = hasher_obj->read_update_files();
    if (status_ok == -1) {
        cout << "Could not properly read update files." << endl;
        return -1;
    }
    // This overwrite top_feature
    hasher_obj->fill_data_nums_accum();
    status_ok = hasher_obj->load_itq_model();
    if (status_ok == -1) {
        cout << "Could not properly load ITQ model." << endl;
        return -1;
    }
    status_ok = hasher_obj->load_hashcodes();
    if (status_ok == -1) {
        cout << "Could not properly load hashcodes." << endl;
        return -1;
    }

    // Initializing features structures and files streams
    hasher_obj->init_feature_mat();

    string query_filename = argv[1];
    string outname(query_filename);
    outname.resize(outname.size()-4);
    hasher_obj->set_outputfile(outname);

    // Read query
    Mat query_mat = hasher_obj->read_feats_from_disk(query_filename);
    // Compute KNN and save results to outfile based on outname
    hasher_obj->find_knn_from_feats(query_mat);
    
    // Clean up
    hasher_obj->clean_compfeat_files();

    
    // // 1. Time reading query
    // cout << "Time reading query (seconds): " << runtimes[0] << std::endl;
    // // 2. Time reading files list
    // cout << "Time reading files list (seconds): " << runtimes[1] << std::endl;
    // // 3. Time filling files structures
    // cout << "Time read DB hash codes (seconds): " << runtimes[7] << std::endl;
    // cout << "Time filling comp files structures (seconds): " << runtimes[2] << std::endl;
    // // 4. Time gettting query hash code(s)
    // cout << "Time gettting query hash code(s) (seconds): " << runtimes[3] << std::endl;
    // // 5. Hamming distances (accumulate for all queries)
    // cout << "Time hamming distances computation (accumulated for all queries) (seconds): " << runtimes[4] << std::endl;
    // // 6. Reranking (accumulate for all queries)
    // cout << "Time reranking (accumulated for all queries) (seconds): " << runtimes[5] << std::endl;
    // // 7. Time cleaning
    // cout << "Time cleaning (seconds): " << runtimes[6] << std::endl;
    // // 8. Total
    // cout << "Total time (seconds): " << (float)(get_wall_time() - t[0]) << std::endl;
    return 0;
}
