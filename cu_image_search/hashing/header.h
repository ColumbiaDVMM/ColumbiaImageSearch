#define USE_OMP

#include <string.h> 
#include <iostream>

double get_wall_time();

std::string base_modelpath = "/home/ubuntu/memex/";
std::string base_updatepath = "/home/ubuntu/memex/update/";

// Trying to gather all hard coded path or part of path here
std::string update_files_listname = "update_list_dev.txt";
std::string update_hash_folder = "hash_bits/";
std::string update_feature_folder = "features/";
std::string update_compfeature_folder = "comp_features/";
std::string update_compidx_folder = "comp_idx/";

// Initialize
std::string update_files_list = base_updatepath+update_files_listname;
std::string update_hash_prefix = base_updatepath+update_hash_folder;
std::string update_feature_prefix = base_updatepath+update_feature_folder;
std::string update_compfeature_prefix = base_updatepath+update_compfeature_folder;
std::string update_compidx_prefix = base_updatepath+update_compidx_folder;

// To be called when set from command line calls
void set_paths() {
    update_files_list = base_updatepath+update_files_listname;
    update_hash_prefix = base_updatepath+update_hash_folder;
    update_feature_prefix = base_updatepath+update_feature_folder;
    update_compfeature_prefix = base_updatepath+update_compfeature_folder;
    update_compidx_prefix = base_updatepath+update_compidx_folder;
}

// Additional output of hamming distances if DEMO==0
#define DEMO 1

int NumberOfSetBits(unsigned int i)
{
    i = i - ((i >> 1) & 0x55555555);
    i = (i & 0x33333333) + ((i >> 2) & 0x33333333);
    return (((i + (i >> 4)) & 0x0F0F0F0F) * 0x01010101) >> 24;
}

typedef std::pair<int,int> mypair;
typedef std::pair<float,int> mypairf;

bool comparator ( const mypair & l, const mypair & r)
{ return l.first < r.first; }

bool comparatorf ( const mypairf & l, const mypairf & r)
{ return l.first < r.first; }


