#include <stdio.h>
#include <string>

// Gathering all filenames related string in this class
class PathManager {

 	public:
 		PathManager() {
 			set_default_paths();
 		}

	 	void set_paths(int norm, int bit_num);
	 	void set_default_paths();

	 	std::string base_modelpath;
		std::string base_updatepath;
		
		std::string str_norm;
		std::string bit_string;

	 	std::string update_files_listname;
		std::string update_files_list;

		// Hashcodes files
		std::string update_hash_folder;
		std::string update_hash_prefix;
    	std::string update_hash_suffix;
		// Compressed features files
		std::string update_compfeature_folder;
		std::string update_compfeature_prefix;
    	std::string update_compfeature_suffix;
    	// Compressed features index (start-beginning)
		std::string update_compidx_folder;
		std::string update_compidx_prefix;
    	std::string update_compidx_suffix;
    	// Features (uncompressed)
		std::string update_feature_folder;
		std::string update_feature_prefix;
		std::string update_feature_suffix;

		// DB hashcodes file
    	std::string itq_name;
    	// ITQ model files
        std::string W_name;
        std::string mvec_name;
        
};

