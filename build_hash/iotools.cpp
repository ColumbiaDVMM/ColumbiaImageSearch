#include "iotools.h"
#include "zlib.h"
#include <stdio.h>

// Compression functions
int compress_onefeat(char * in, char * comp, int fsize) {
    // Struct init
    z_stream defstream;
    defstream.zalloc = Z_NULL;
    defstream.zfree = Z_NULL;
    defstream.opaque = Z_NULL;
    defstream.avail_in = (uInt)fsize; // size of input
    defstream.next_in = (Bytef *)in; // input char array
    defstream.avail_out = (uInt)fsize; // size of output
    defstream.next_out = (Bytef *)comp; // output char array
    
    // the actual compression work.
    deflateInit(&defstream, Z_BEST_COMPRESSION);
    deflate(&defstream, Z_FINISH);
    deflateEnd(&defstream);
    return defstream.total_out;
}

int decompress_onefeat(char * in, char * comp, int compsize, int fsize) {
    // Struct init
    z_stream infstream;
    infstream.zalloc = Z_NULL;
    infstream.zfree = Z_NULL;
    infstream.opaque = Z_NULL;
    infstream.avail_in = (uInt)compsize; // size of input
    infstream.next_in = (Bytef *)in; // input char array
    infstream.avail_out = (uInt)fsize; // size of output
    infstream.next_out = (Bytef *)comp; // output char array
    
    // the actual de-compression work.
    inflateInit(&infstream);
    inflate(&infstream, Z_NO_FLUSH);
    inflateEnd(&infstream);
    return infstream.total_out;
}

// File reading functions
std::ifstream::pos_type filesize(std::string filename)
{
	std::ifstream in(filename, std::ios::ate | std::ios::binary);
	return in.tellg(); 
}

int get_file_pos(int * accum, int nb_files, int query, int & res)
{
    int file_id = 0;    
    while (query >= accum[file_id] && file_id<nb_files)
    {
        file_id++;
    }
    if (file_id==nb_files) {
        res=-1;
        std::cout << "Could not find feature "  << query << ", maximum id is " << accum[file_id-1] << std::endl;
        return -1;
    }

    if (!file_id)
        res = query;
    else
        res = query-accum[file_id-1];
    return file_id;
}

int get_onefeatcomp(int query_id, size_t read_size, int* accum, vector<ifstream*>& read_in_compfeatures, vector<ifstream*>& read_in_compidx, char* feature_cp) {
    int new_pos = 0;
    int file_id = 0;
    size_t idx_size = sizeof(unsigned long long int);
    unsigned long long int start_feat,end_feat;
    char* comp_feature = new char[read_size];
    file_id = get_file_pos(accum, (int)read_in_compidx.size(), query_id, new_pos);
    if (file_id==-1)
        return -1;
    std::cout << "Feature found in file "  << file_id << " at pos " << new_pos << std::endl;
    read_in_compidx[file_id]->seekg((unsigned long long int)(new_pos)*idx_size);
    read_in_compidx[file_id]->read((char*)&start_feat, idx_size);
    read_in_compidx[file_id]->read((char*)&end_feat, idx_size);
    read_in_compfeatures[file_id]->seekg(start_feat);
    read_in_compfeatures[file_id]->read(comp_feature, end_feat-start_feat);
    decompress_onefeat(comp_feature, feature_cp, (int)end_feat-start_feat, read_size);
    delete[] comp_feature;
    return 0;
}

int get_onefeat(int query_id, size_t read_size, int* accum, vector<ifstream*>& read_in_features, char* feature_cp) {
    int new_pos = 0;
    int file_id = 0;
    file_id = get_file_pos(accum, (int)read_in_features.size(), query_id, new_pos);
    if (file_id==-1)
        return -1;
    std::cout << "Feature found in file "  << file_id << " at pos " << new_pos << std::endl;
    read_in_features[file_id]->seekg((unsigned long long int)(new_pos)*read_size);
    read_in_features[file_id]->read(feature_cp, read_size);    
    return 0;
}

unsigned long long int fill_data_nums(vector<string>& update_hash_files, vector<unsigned long long int>& data_nums, int bit_num) {
    unsigned long long int data_num = 0;
    for (int i=0;i<update_hash_files.size();i++)
    {
        data_nums.push_back((unsigned long long int)filesize(update_hash_files[i])*8/bit_num);
        data_num += data_nums[i];
	std::cout << "We have a " << data_nums[i] << " features in file " << update_hash_files[i] << std::endl;
    }
    std::cout << "We have a total of " << data_num << " features." << std::endl;
    return data_num;
}

int fill_vector_files(vector<ifstream*>& read_in, vector<string>& update_files){
    for (int i=0;i<update_files.size();i++)
    {
        read_in.push_back(new ifstream);
        read_in[i]->open(update_files[i],ios::in|ios::binary);
        if (!read_in[i]->is_open())
            {
                std::cout << "Cannot load the file " << update_files[i] << std::endl;
                return -1;
            } 
    }
    return 0;
}

void fill_accum(vector<unsigned long long int>& data_nums, int * accum) {
    accum[0]=data_nums[0];
    for (int i=1;i<data_nums.size();i++)
    {
        accum[i]=accum[i-1]+data_nums[i];
    }
}

int get_n_features(string udpate_fn, int* query_ids, int query_num, int norm, int bit_num, size_t read_size, char* feature_cp) {
    // Some ugly hard coded string initialization...
    string bit_string = to_string((long long)bit_num);
    string str_norm = "";
    if (norm)
        str_norm = "_norm";
    string itq_name = "itq" + str_norm + "_" + bit_string;
    string update_compfeature_suffix = "_comp" + str_norm;
    string update_compidx_suffix = "_compidx" + str_norm;
    string update_hash_suffix = "";
    if (norm)
    {
        update_hash_suffix = "_" + itq_name;
    }

    // Config update
    string line;
    vector<string> update_hash_files;
    vector<string> update_compfeature_files;
    vector<string> update_compidx_files;
    ifstream fu(udpate_fn,ios::in);
    if (!fu.is_open())
    {
        std::cout << "no update" << std::endl;
    }
    else
    {
        while (getline(fu, line)) {
            update_hash_files.push_back(update_hash_prefix+line+update_hash_suffix);
            update_compfeature_files.push_back(update_compfeature_prefix+line+update_compfeature_suffix);
            update_compidx_files.push_back(update_compidx_prefix+line+update_compidx_suffix);
        }
    }
    
    // get compressed features files pointers and their corresponding indices
    vector<ifstream*> read_in_compfeatures;
    vector<ifstream*> read_in_compidx;
    int status = 0;
    status = fill_vector_files(read_in_compfeatures,update_compfeature_files);
    if (status==-1) {
        std::cout << "Could not load compressed features properly. Exiting." << std::endl;
        // TODO: We should clean here
        return -1;
    }
    status = fill_vector_files(read_in_compidx,update_compidx_files);
    if (status==-1) {
        std::cout << "Could not load compressed indices properly. Exiting." << std::endl;
        // TODO: We should clean here
        return -1;
    }

    // read in all files size to know where to look for features...
    vector<unsigned long long int> data_nums;
    unsigned long long int data_num = fill_data_nums(update_hash_files,data_nums,bit_num);
    int * accum = new int[data_nums.size()];
    fill_accum(data_nums,accum);

    // Get query feature(s)
    for (int i=0;i<query_num;i++)
    {
        std::cout << "Looking for feature #" << query_ids[i] << std::endl;
        // BEWARE: we consider here ids are python/db, so in C they are ids+1...
        // TODO: maybe define a flag python id or not
        status = get_onefeatcomp(query_ids[i]-1,read_size,accum,read_in_compfeatures,read_in_compidx,feature_cp);
	if (status==-1) {
        	std::cout << "Could not load compressed feature " << query_ids[i]-1 << ". Exiting." << std::endl;
        	// TODO: We should clean here
        	return -1;
   	}

        feature_cp +=read_size;
    }
    
    // clean exit
    delete[] accum;
    for (int i = 1; i<data_nums.size();i++)
    {
        read_in_compfeatures[i]->close();
        read_in_compidx[i]->close();
        delete read_in_compfeatures[i];
        delete read_in_compidx[i];
    }
    return 0;
}
