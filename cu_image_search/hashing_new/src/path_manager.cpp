#ifndef PATH_MANAGER_H
#define PATH_MANAGER_H

#include "path_manager.hpp"

void PathManager::set_paths(int norm, int bit_num) {

	update_files_list = base_updatepath+update_files_listname;
    
    bit_string = std::to_string((long long)bit_num);
    if (norm)
    	str_norm = "_norm";
    else
    	str_norm = "";
    
    update_hash_prefix = base_updatepath+update_hash_folder;
    update_hash_suffix = "_itq" + str_norm + "_" + bit_string;
    
    update_compidx_prefix = base_updatepath+update_compidx_folder;
    update_compidx_suffix = "_compidx" + str_norm;
    
    update_compfeature_prefix = base_updatepath+update_compfeature_folder;
    update_compfeature_suffix = "_comp" + str_norm;
    
    update_feature_prefix = base_updatepath+update_feature_folder;
    update_feature_suffix = "" + str_norm;
}

void PathManager::set_default_paths() {
	base_modelpath = "/home/ubuntu/memex/";
	base_updatepath = "/home/ubuntu/memex/update/";

	update_files_listname = "update_list_dev.txt";
	update_hash_folder = "hash_bits/";
	update_feature_folder = "features/";
	update_compfeature_folder = "comp_features/";
	update_compidx_folder = "comp_idx/";
	
	// default is norm, 256 bits
	set_paths(1, 256);
}

#endif