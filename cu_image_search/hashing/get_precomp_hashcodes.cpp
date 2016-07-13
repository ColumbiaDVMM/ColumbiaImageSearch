#include "header.h"
#include <opencv2/opencv.hpp>
#include "iotools.h"
#include <fstream>

using namespace std;
using namespace cv;

int main(int argc, char** argv){
    double t[2]; // timing
    t[0] = get_wall_time(); // Start Time
    if (argc < 3){
        cout << "Usage: get_precomp_hashcodes feature_ids_file_name hashcodes_file_name [base_updatepath num_bits normalize_features]" << std::endl;
        return -1;
    }
    //omp_set_num_threads(omp_get_max_threads());

    // hardcoded default value. 
    int norm = true;
    int bit_num = 256; 
    string ids_file(argv[1]);
    string out_file(argv[2]);
    if (argc>3)
        base_updatepath = argv[3];
    set_paths();
    if (argc>4)
        bit_num = atoi(argv[4]);
    if (argc>5)
        norm = atoi(argv[5]);

    //read in query
    int query_num = (int)filesize(argv[1])/sizeof(int);
    ifstream read_in(argv[1],ios::in|ios::binary);
    if (!read_in.is_open())
    {
        std::cout << "Cannot load the query feature ids file!" << std::endl;
        return -1;
    }
    // read query ids
    int* query_ids = new int[query_num];
    size_t read_size = sizeof(int)*query_num;
    read_in.read((char*)query_ids, read_size);
    read_in.close();

    read_size = sizeof(int)*bit_num/32;
    int* hashcodes = new int[query_num*read_size];
    char* hashcodes_cp = (char*)hashcodes;

    int status = get_n_hashcodes(update_files_list,query_ids,query_num,norm,bit_num,read_size,hashcodes_cp);
    if (status==-1) {
        std::cout << "Could not get hashcodes. Exiting." << std::endl;
        // TODO: We should clean here
        return -1;
    }
    // write out features to out_file
    //cout << "Will write feature to " << out_file << endl;
    ofstream output(out_file,ofstream::binary);
    hashcodes_cp = (char*)hashcodes;
    for (int i = 0; i<query_num; i++) {
        output.write(hashcodes_cp,read_size);
        hashcodes_cp +=  read_size;
    }
    output.close();
    cout << "Hashcodes saved to " << out_file << endl;

    // Cleaning
    delete[] query_ids;
    delete[] hashcodes;

    cout << "[get_precomp_hashcodes] Total time (seconds): " << (float)(get_wall_time() - t[0]) << std::endl;
    return 0;
}

