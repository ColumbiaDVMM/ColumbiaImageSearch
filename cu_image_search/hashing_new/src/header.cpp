#include "header.h"

extern std::string base_modelpath;
extern std::string base_updatepath;
extern std::string update_files_listname;
extern std::string update_hash_folder;
extern std::string update_feature_folder;
extern std::string update_compfeature_folder;
extern std::string update_compidx_folder;
extern std::string update_files_list;
extern std::string update_hash_prefix;
extern std::string update_feature_prefix;
extern std::string update_compfeature_prefix;
extern std::string update_compidx_prefix;

#ifdef _WIN32
#include <time.h>
double get_wall_time(){
    double t = (double)clock()/CLOCKS_PER_SEC;
    return t;
}
#else
#include <cstddef>
#include <sys/time.h>
double get_wall_time(){
    struct timeval time;
    if (gettimeofday(&time,NULL)){
        //  Handle error
        return 0;
    }
    return (double)time.tv_sec + (double)time.tv_usec * .000001;
}
#endif

void set_paths() {
    update_files_list = base_updatepath+update_files_listname;
    update_hash_prefix = base_updatepath+update_hash_folder;
    update_feature_prefix = base_updatepath+update_feature_folder;
    update_compfeature_prefix = base_updatepath+update_compfeature_folder;
    update_compidx_prefix = base_updatepath+update_compidx_folder;
}

void set_default_paths() {
	base_modelpath = "/home/ubuntu/memex/";
	base_updatepath = "/home/ubuntu/memex/update/";

	update_files_listname = "update_list_dev.txt";
	update_hash_folder = "hash_bits/";
	update_feature_folder = "features/";
	update_compfeature_folder = "comp_features/";
	update_compidx_folder = "comp_idx/";
	set_paths();
}