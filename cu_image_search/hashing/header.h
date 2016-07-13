#define USE_OMP

#include <string.h> 
#include <iostream>

double get_wall_time();

std::string base_modelpath = "/home/ubuntu/memex/";
std::string base_updatepath = "/home/ubuntu/memex/update/";

// Trying to gather all hard coded path or part of path here
std::string update_files_list = base_updatepath+"update_list_dev.txt";
std::string update_hash_prefix = base_updatepath+"hash_bits/";
std::string update_feature_prefix = base_updatepath+"features/";
std::string update_compfeature_prefix = base_updatepath+"comp_features/";
std::string update_compidx_prefix = base_updatepath+"comp_idx/";

void set_paths() {
    update_files_list = base_updatepath+"update_list_dev.txt";
    update_hash_prefix = base_updatepath+"hash_bits/";
    update_feature_prefix = base_updatepath+"features/";
    update_compfeature_prefix = base_updatepath+"comp_features/";
    update_compidx_prefix = base_updatepath+"comp_idx/";
}
