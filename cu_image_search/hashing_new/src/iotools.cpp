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
ifstream::pos_type filesize(string filename)
{
    ifstream in(filename, ios::ate | ios::binary);
    if (!in.fail()) {
        return in.tellg();
    } else {
        return 0;
    }
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
        cout << "Could not find feature "  << query << ", maximum id is " << accum[file_id-1] << endl;
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
    //cout << "Feature found in file "  << file_id << " at pos " << new_pos << endl;
    read_in_compidx[file_id]->seekg((unsigned long long int)(new_pos)*idx_size);
    read_in_compidx[file_id]->read((char*)&start_feat, idx_size);
    read_in_compidx[file_id]->read((char*)&end_feat, idx_size);
    read_in_compfeatures[file_id]->seekg(start_feat);
    read_in_compfeatures[file_id]->read(comp_feature, end_feat-start_feat);
    //cout << "Reading compressed feature from "  << start_feat << " to " << end_feat << endl;
    uLong total_out = decompress_onefeat(comp_feature, feature_cp, (int)end_feat-start_feat, read_size);
    //cout << "Decompressed size is "  << total_out << endl;
    delete[] comp_feature;
    return 0;
}

int get_onesample(int query_id, size_t read_size, int* accum, vector<ifstream*>& read_in, char* cp) {
    int new_pos = 0;
    int file_id = 0;
    file_id = get_file_pos(accum, (int)read_in.size(), query_id, new_pos);
    if (file_id==-1)
        return -1;
    //cout << "Feature found in file "  << file_id << " at pos " << new_pos << endl;
    read_in[file_id]->seekg((unsigned long long int)(new_pos)*read_size);
    read_in[file_id]->read(cp, read_size);
    return 0;
}

unsigned long long int fill_data_nums(vector<string>& update_hash_files, vector<unsigned long long int>& data_nums, int bit_num) {
    unsigned long long int data_num = 0;
    for (int i=0;i<update_hash_files.size();i++)
    {
        data_nums.push_back((unsigned long long int)filesize(update_hash_files[i])*8/bit_num);
        data_num += data_nums[i];
        //cout << "We have a " << data_nums[i] << " features in file " << update_hash_files[i] << endl;
    }
    cout << "We have a total of " << data_num << " features." << endl;
    return data_num;
}

int fill_vector_files(vector<ifstream*>& read_in, vector<string>& update_files){
    for (int i=0;i<update_files.size();i++)
    {
    read_in.push_back(new ifstream(update_files[i],ios::in|ios::binary));
    /*read_in.push_back(new ifstream);
    read_in[i]->open(update_files[i],ios::in|ios::binary);*/
    if (!read_in[i]->is_open())
        {
            cout << "Cannot load the file " << update_files[i] << endl;
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

int get_n_features(int* query_ids, int query_num, int norm, int bit_num, size_t read_size, char* feature_cp, PathManager& pm) {
    
    string line;
    vector<string> update_hash_files;
    vector<string> update_compfeature_files;
    vector<string> update_compidx_files;
    
    cout << "Reading update file: " << pm.update_files_list << endl;
    ifstream fu(pm.update_files_list,ios::in);
    if (!fu.is_open())
    {
        cout << "No update! Was looking for " << pm.update_files_list << endl;
        return -1;
    }
    else
    {
        while (getline(fu, line)) {
            //cout << "Loading update: " << line << endl;
            cout << "Adding hashcode file " << pm.update_hash_prefix+line+pm.update_hash_suffix << endl;
            update_hash_files.push_back(pm.update_hash_prefix+line+pm.update_hash_suffix);
            update_compfeature_files.push_back(pm.update_compfeature_prefix+line+pm.update_compfeature_suffix);
            update_compidx_files.push_back(pm.update_compidx_prefix+line+pm.update_compidx_suffix);
        }
    }

    // get compressed features files pointers and their corresponding indices
    vector<ifstream*> read_in_compfeatures;
    vector<ifstream*> read_in_compidx;
    int status = 0;
    status = fill_vector_files(read_in_compfeatures, update_compfeature_files);
    if (status==-1) {
        cout << "Could not load compressed features properly. Exiting." << endl;
        // TODO: We should clean here
        return -1;
    }
    status = fill_vector_files(read_in_compidx, update_compidx_files);
    if (status==-1) {
        cout << "Could not load compressed indices properly. Exiting." << endl;
        // TODO: We should clean here
        return -1;
    }

    // read in all files size to know where to look for features...
    vector<unsigned long long int> data_nums;
    unsigned long long int data_num = fill_data_nums(update_hash_files, data_nums, bit_num);
    int * accum = new int[data_nums.size()];
    fill_accum(data_nums, accum);

    // Get query feature(s)
    cout << "Looking for " << query_num << " features..." << endl;
    for (int i=0;i<query_num;i++)
    {
        //cout << "Looking for feature #" << query_ids[i] << endl;
        // BEWARE: we consider here ids are python/db, so in C they are ids+1...
        // TODO: maybe define a flag python id or not
        status = get_onefeatcomp(query_ids[i]-1,read_size,accum,read_in_compfeatures,read_in_compidx,feature_cp);
        if (status==-1) {
            cout << "Could not load compressed feature " << query_ids[i]-1 << ". Exiting." << endl;
            // TODO: We should clean here
            return -1;
        }
        feature_cp +=read_size;
    }
    //cout << "Finished looking for " << query_num << " features." << endl;
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

int get_n_hashcodes(int* query_ids, int query_num, int norm, int bit_num, size_t read_size, char* hashcode_cp, PathManager& pm) {
    
    string line;
    vector<string> update_hash_files;
    vector<string> update_compfeature_files;
    vector<string> update_compidx_files;

    cout << "Reading update file: " << pm.update_files_list << endl;
    ifstream fu(pm.update_files_list,ios::in);
    if (!fu.is_open())
    {
        cout << "No update! Was looking for " << pm.update_files_list << endl;
    return -1;
    }
    else
    {
        while (getline(fu, line)) {
            //cout << "Loading update: " << line << endl;
            update_hash_files.push_back(pm.update_hash_prefix+line+pm.update_hash_suffix);
        }
    }

    // get compressed features files pointers and their corresponding indices
    vector<ifstream*> read_in_hashcodes;
    int status = 0;
    status = fill_vector_files(read_in_hashcodes, update_hash_files);
    if (status==-1) {
        cout << "Could not load hashcodes properly. Exiting." << endl;
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
        cout << "Looking for feature #" << query_ids[i] << endl;
        // BEWARE: we consider here ids are python/db, so in C they are ids+1...
        // TODO: maybe define a flag python id or not
        status = get_onesample(query_ids[i]-1,read_size,accum,read_in_hashcodes,hashcode_cp);
        if (status==-1) {
            cout << "Could not load hashcode " << query_ids[i]-1 << ". Exiting." << endl;
            // TODO: We should clean here
            return -1;
        }
        hashcode_cp +=read_size;
    }

    // clean exit
    delete[] accum;
    for (int i = 1; i<data_nums.size();i++)
    {
        read_in_hashcodes[i]->close();
        delete read_in_hashcodes[i];
    }
    return 0;
}
