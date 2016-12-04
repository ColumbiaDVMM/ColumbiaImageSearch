#include "header.h"
#include "iotools.h"

#include <opencv2/opencv.hpp>
#include <fstream>

using namespace std;
using namespace cv;


int main(int argc, char** argv){
    double t[2]; // timing
    t[0] = get_wall_time(); // Start Time
    float runtimes[6] = {0.0f,0.0f,0.0f,0.0f,0.0f,0.0f};
    if (argc < 3){
        cout << "Usage: hashing feature_file_name feature_number [base_modelpath base_updatepath hashing_bits]" << std::endl;
        return -1;
    }
    
    // Deal with parameters
    PathManager pm;
    int feature_dim = 4096;
    int bit_num = 256;
    int norm = true;
    if (argc>3)
        pm.base_modelpath = argv[3];
    if (argc>4)
        pm.base_updatepath = argv[4];
    if (argc>5)
        bit_num = atoi(argv[5]);
    int int_num = bit_num/32;
    pm.set_paths(norm, bit_num);
    
    //read in query
    int query_num = atoi(argv[2]);
    ifstream read_in(argv[1],ios::in|ios::binary);
    if (!read_in.is_open())
    {
        std::cout << "Cannot load the query feature file!" << std::endl;
        return -1;
    }
    Mat query_mat(query_num,feature_dim,CV_32F);
    size_t read_size = sizeof(float)*feature_dim*query_num;
    read_in.read((char*)query_mat.data, read_size);
    read_in.close();

    //read in model
    read_in.open(pm.W_name,ios::in|ios::binary);
    if (!read_in.is_open())
    {
        std::cout << "Cannot load the W model from: " << pm.W_name << std::endl;
        return -1;
    }
    Mat W(feature_dim,bit_num,CV_64F);
    read_size = sizeof(double)*feature_dim*bit_num;
    read_in.read((char*)W.data, read_size);
    read_in.close();

    read_in.open(pm.mvec_name,ios::in|ios::binary);
    if (!read_in.is_open())
    {
        std::cout << "Cannot load the mvec model " << pm.mvec_name << std::endl;
        return -1;
    }
    Mat mvec(1,bit_num,CV_64F);
    read_size = sizeof(double)*bit_num;
    read_in.read((char*)mvec.data, read_size);
    read_in.close();


    runtimes[0]=(float)(get_wall_time() - t[0]);

    //hashing init
    t[1]=get_wall_time();
    if (norm)
    {
        for  (int k=0;k<query_num;k++)
            normalize((float*)query_mat.data+k*feature_dim,feature_dim);
    }
    Mat query_mat_double;
    query_mat.convertTo(query_mat_double, CV_64F);
    mvec = repeat(mvec, query_num,1);
    Mat query_hash = query_mat_double*W-mvec;
    unsigned int * query_all = new unsigned int[int_num*query_num];
    for  (int k=0;k<query_num;k++)
    {
        for (int i=0;i<int_num;i++)
        {
            query_all[k*int_num+i] = 0;
            for (int j=0;j<32;j++)
                if (query_hash.at<double>(k,i*32+j)>0)
                    query_all[k*int_num+i] += 1<<j;
        }
    }

    string filename = argv[1];
    filename.resize(filename.size()-4);
    string outname = filename;
    if (norm)
        outname = outname + "_norm";
    ofstream write_out(outname,ios::out|ios::binary);
    if (!write_out.is_open())
    {
        std::cout << "Cannot open the output feature file for writing!" << std::endl;
        return -1;
    }
    size_t write_size = sizeof(float)*feature_dim*query_num;
    write_out.write((char*)query_mat.data, write_size);
    write_out.close();

    string itq_name = filename + "_itq_" + pm.str_norm + pm.bit_string;
    write_out.open(itq_name,ios::out|ios::binary);
    if (!write_out.is_open())
    {
        std::cout << "Cannot open the output hashing file " << itq_name << " for writing " << std::endl;
        return -1;
    }
    write_size = sizeof(int)*query_num*int_num;
    write_out.write((char*)query_all, write_size);
    write_out.close();


    runtimes[1]=(float)(get_wall_time() - t[1]);

    delete[] query_all;
    read_in.close();

    cout << "loading (seconds): " << runtimes[0] << std::endl;
    cout << "hashing init (seconds): " << runtimes[1] << std::endl;
    cout << "total time (seconds): " << (float)(get_wall_time() - t[0]) << std::endl;
    return 0;
}

