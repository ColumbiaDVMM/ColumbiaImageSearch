#define USE_OMP

#include <string.h> 
#include <iostream>

double get_wall_time();

std::string base_path = "/home/ubuntu/memex";

// Trying to gather all hard coded path or part of path here
std::string update_files_list = base_path+"/update/update_list_dev.txt";
std::string update_hash_prefix = base_path+"/update/hash_bits/";
std::string update_feature_prefix = base_path+"/update/features/";
std::string update_compfeature_prefix = base_path+"/update/comp_features/";
std::string update_compidx_prefix = base_path+"/update/comp_idx/";
