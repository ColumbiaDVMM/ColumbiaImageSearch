#include "header.h"
#include <opencv2/opencv.hpp>
#include <fstream>
#include <assert.h>
#include "iotools.h"
// For mkdir
#include <sys/types.h>
#include <sys/stat.h>

using namespace std;
using namespace cv;

int main(int argc, char** argv){
	double t[2]; // timing
	t[0] = get_wall_time(); // Start Time
	if (argc < 1){
		cout << "Usage: compress_feat [feature_dim normalize_features]" << std::endl;
		return -1;
	}

	// hardcoded default value. Maybe read that from JSON conf
	int feature_dim = 4096;
	int norm = true;
	if (argc>2)
	    feature_dim = atoi(argv[2]);
	if (argc>3)
		norm = atoi(argv[4]);

	string str_norm = "";
	if (norm)
		str_norm = "_norm";

    	// File names vectors, prefix and suffix.
 	string line;
	vector<string> update_feature_files;
	vector<string> update_comp_feature_files;
   	vector<string> update_compidx_files;
   	vector<int> need_comp;
	// mkdir? but no real cross platform of doing this...
	string update_feature_prefix = "update/features/";
   	string update_comp_feature_prefix = "update/comp_features/";
	string update_compidx_prefix = "update/comp_idx/";
   	int status; //Not working on MAC
	status = mkdir(update_comp_feature_prefix.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
	status = mkdir(update_compidx_prefix.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
	string update_feature_suffix = "" + str_norm;
	string update_comp_feature_suffix = "_comp" + str_norm;
	string update_compidx_suffix = "_compidx" + str_norm;

 	// This may be an argument or read from JSON conf
	ifstream fu("update_list.txt",ios::in);
	if (!fu.is_open())
	{
		std::cout << "No update!" << std::endl;
        	return 0;
	}
	else
	{
		while (getline(fu, line)) {
			update_feature_files.push_back(update_feature_prefix+line+update_feature_suffix);
            		update_comp_feature_files.push_back(update_comp_feature_prefix+line+update_comp_feature_suffix);
            		update_compidx_files.push_back(update_compidx_prefix+line+update_compidx_suffix);
		}
	}

    	// Check if we have as many comp files and comp idx as original feature files...
    	int data_num = 0;
    	int idx_num = 0;
	int filesizecomp = 0;
	int filesizeidx = 0;
    	for (int i=0;i<update_comp_feature_files.size();i++)
    	{
		filesizeidx = filesize(update_compidx_files[i]);
		filesizecomp = filesize(update_comp_feature_files[i]);
        	if (filesizecomp==0||filesizecomp==-1||filesizeidx==0||filesizeidx==-1) { 
            		// Comp file empty or non existing
			std:cout << "Some file missing for " << update_comp_feature_files[i] << std::endl;
            		need_comp.push_back(i);
            		continue;
        	}
        	data_num=filesize(update_feature_files[i])/(sizeof(float)*feature_dim);
        	idx_num=filesize(update_compidx_files[i])/sizeof(unsigned long long int);
        	if (idx_num-1!=data_num) {
			// We have a mismatch indices vs features 
	        	std::cout << "Curr feat size: " << data_num << " (feat file size: " << filesize(update_feature_files[i]) << "), curr idx size: " << idx_num  << " (compidx file size: " << filesize(update_compidx_files[i]) << ")" << endl;
    		        need_comp.push_back(i);
		}
   	}
    	// ... if so we should be good to go.
    	if (need_comp.size()==0) {
        	std::cout << "Everything seems up-to-date, exiting." << std::endl;
        	return 0;        
    	} else {
        	std::cout << "We need to compress " << need_comp.size() << " files." << std::endl;
	}

    ifstream read_in;
    ofstream comp_out,comp_idx;
    int read_size = 0;
    int comp_size = 0;
    // Compress needed files.
    for (int cfi=0;cfi<need_comp.size();cfi++) {
        // Read features
	int fi = need_comp[cfi];
        read_in.open(update_feature_files[fi],ios::in|ios::binary);
        comp_out.open(update_comp_feature_files[fi],ios::out|ios::binary);
        comp_idx.open(update_compidx_files[fi],ios::out|ios::binary);
        if (!read_in.is_open())
        {
            std::cout << "Cannot read features file: " << update_feature_files[fi] << std::endl;
            return -1;
        }
        data_num = (filesize(update_feature_files[fi])/(sizeof(float)*feature_dim));
        read_size = sizeof(float)*feature_dim;
        // Memory should be recycled.
        char* feature = new char[read_size];
        char* comp_feature = new char[read_size];
        unsigned long long int curr_pos = 0;
        comp_idx.write((char *)&curr_pos,sizeof(unsigned long long int));
        std::cout << "We need to compress " << data_num << " features for file " << update_feature_files[fi] << std::endl;
        // Compress each feature separately, write it out along its compressed size
        for (int feat_num=0;feat_num<data_num;feat_num++) {
            read_in.read(feature, read_size);
            // if (norm) {
            //     normalize((float*)feature,feature_dim);
            // }
            comp_size = compress_onefeat(feature,comp_feature,read_size);
            comp_out.write(comp_feature,comp_size);
            curr_pos+=comp_size;
            comp_idx.write((char *)&curr_pos,sizeof(unsigned long long int));
        }
        delete feature;
        delete comp_feature;
        read_in.close();  
        comp_out.close();
        comp_idx.close();
    }
    
	cout << "Total time (seconds): " << (float)(get_wall_time() - t[0]) << std::endl;
	return 0;
}

