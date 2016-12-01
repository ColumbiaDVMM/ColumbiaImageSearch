#ifndef HASHHEADER_H
#define HASHHEADER_H

#define USE_OMP
// Additional output of hamming distances if DEMO==0
#define DEMO 1

#include <string> 
#include <iostream>

double get_wall_time();

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

// // Trying to gather all paths or part of path here
// std::string base_modelpath;
// std::string base_updatepath;
// std::string update_files_listname;
// std::string update_hash_folder;
// std::string update_feature_folder;
// std::string update_compfeature_folder;
// std::string update_compidx_folder;
// std::string update_files_list;
// std::string update_hash_prefix;
// std::string update_feature_prefix;
// std::string update_compfeature_prefix;
// std::string update_compidx_prefix;

void set_paths();
void set_default_paths();

inline int NumberOfSetBits(unsigned int i)
{
    i = i - ((i >> 1) & 0x55555555);
    i = (i & 0x33333333) + ((i >> 2) & 0x33333333);
    return (((i + (i >> 4)) & 0x0F0F0F0F) * 0x01010101) >> 24;
}

typedef std::pair<int,int> mypair;
typedef std::pair<float,int> mypairf;

inline bool comparator (const mypair & l, const mypair & r) { return l.first < r.first; }

inline bool comparatorf (const mypairf & l, const mypairf & r) { return l.first < r.first; }

#endif