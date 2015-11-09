#define USE_OMP

#include <string.h> 
#include <iostream>

double get_wall_time();

// Trying to gather all hard coded path or part of path here
std::string update_hash_prefix = "update/hash_bits/";
std::string update_feature_prefix = "update/features/";
std::string update_compfeature_prefix = "update/comp_features/";
std::string update_compidx_prefix = "update/comp_idx/";
