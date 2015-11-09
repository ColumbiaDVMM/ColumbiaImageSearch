#include "header.h"
#include "iotools.h"
#include <opencv2/opencv.hpp>
#include <fstream>

using namespace std;
using namespace cv;

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


int main(int argc, char** argv){
	double t[2]; // timing
	t[0] = get_wall_time(); // Start Time
	float runtimes[6] = {0.0f,0.0f,0.0f,0.0f,0.0f,0.0f};
	if (argc < 2){
		cout << "Usage: hashing feature_file_name [hashing_bits post_ranking_ratio nomarlize_features read_threshold]" << std::endl;

		return -1;
	}
	//omp_set_num_threads(omp_get_max_threads());

	// hardcoded
	int feature_dim = 4096;
	float ratio = 0.001f;
	int bit_num = 256;
	int norm = true;
	if (argc>2)
		bit_num = atoi(argv[2]);
	if (argc>3)
		ratio = (float)atof(argv[3]);
	if (argc>4)
		norm = atoi(argv[4]);

	int read_thres = (int)(1.0f/ratio);
	if (argc>5)
		read_thres =  atoi(argv[5]);
	int int_num = bit_num/32;
	string bit_string = to_string((long long)bit_num);
	string str_norm = "";
	if (norm)
		str_norm = "_norm";
	string itq_name = "itq" + str_norm + "_" + bit_string;
	string W_name = "W" + str_norm + "_" + bit_string;
	string mvec_name = "mvec" + str_norm + "_" + bit_string;

	//read in query
	int	query_num = (int)filesize(argv[1])/4/feature_dim;
	std::cout << "Hashing for " << query_num << " queries." << std::endl;
	ifstream read_in(argv[1],ios::in|ios::binary);
	if (!read_in.is_open())
	{
		std::cout << "Cannot load the query feature file!" << std::endl;
		return -1;
	}
	Mat query_mat(query_num,feature_dim,CV_32F);
	size_t read_size = sizeof(float)*feature_dim*query_num;
	read_in.read((char*)query_mat.data, read_size);
	std::cout << "Read " << read_size <<  " bytes for " << query_num << " queries." << std::endl;
	read_in.close();

	//config update
	string line;
	vector<string> update_hash_files;
	vector<string> update_feature_files;
	vector<string> update_compfeature_files;
   	vector<string> update_compidx_files;
	string update_feature_suffix = "" + str_norm;
	string update_compfeature_suffix = "_comp" + str_norm;
	string update_compidx_suffix = "_compidx" + str_norm;
	string update_hash_suffix = "";
	if (norm)
	{
		update_hash_suffix = "_" + itq_name;
	}
	ifstream fu("update_list.txt",ios::in);
	if (!fu.is_open())
	{
		std::cout << "no update" << std::endl;
	}
	else
	{
		while (getline(fu, line)) {
			update_hash_files.push_back(update_hash_prefix+line+update_hash_suffix);
			update_feature_files.push_back(update_feature_prefix+line+update_feature_suffix);
			update_compfeature_files.push_back(update_compfeature_prefix+line+update_compfeature_suffix);
			update_compidx_files.push_back(update_compidx_prefix+line+update_compidx_suffix);
		}
	}
	// read in itq
	vector<unsigned long long int> data_nums;
    unsigned long long int data_num = fill_data_nums(update_hash_files,data_nums,bit_num);
    int * accum = new int[data_nums.size()];
    fill_accum(data_nums,accum);
	int top_feature=(int)ceil(data_num*ratio);
	std::cout << "Will retrieve the top " << top_feature << " features." << std::endl;
	//std::cout << "Loading itq..." << std::endl;
	Mat itq(data_num,int_num,CV_32SC1);
	char * read_pos = (char*)itq.data;
	for (int i=0;i<update_hash_files.size();i++)
	{
		read_in.open(update_hash_files[i],ios::in|ios::binary);
		if (!read_in.is_open())
		{
			std::cout << "Cannot load the itq updates! File "<< update_hash_files[i] << std::endl;
			return -1;
		}
		read_size = sizeof(int)*data_nums[i]*int_num;
		read_in.read(read_pos, read_size);
		read_in.close();
		read_pos +=read_size;
	} 

	read_in.open(W_name,ios::in|ios::binary);
	if (!read_in.is_open())
	{
		std::cout << "Cannot load the W model!" << std::endl;
		return -1;
	}
	Mat W(feature_dim,bit_num,CV_64F);
	read_size = sizeof(double)*feature_dim*bit_num;
	read_in.read((char*)W.data, read_size);
	read_in.close();

	read_in.open(mvec_name,ios::in|ios::binary);
	if (!read_in.is_open())
	{
		std::cout << "Cannot load the mvec model!" << std::endl;
		return -1;
	}
	Mat mvec(1,bit_num,CV_64F);
	read_size = sizeof(double)*bit_num;
	read_in.read((char*)mvec.data, read_size);
	read_in.close();

	
	// Initializing features structures and files streams
	Mat feature;
	feature.create(top_feature,feature_dim,CV_32F);

	vector<ifstream*> read_in_compfeatures;
	vector<ifstream*> read_in_features;
	vector<ifstream*> read_in_compidx;
	int status = 0;
    status = fill_vector_files(read_in_features,update_feature_files);
    if (status==-1) {
        std::cout << "Could not load features properly. Exiting." << std::endl;
        // TODO: We should clean here
        return -1;
    }
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
	vector<mypair> hamming(data_num);
	vector<mypairf> postrank(top_feature);
	string outname = argv[1];
	outname.resize(outname.size()-4);
	string outname_sim = outname+"-sim.txt";
	ofstream outputfile;
	outputfile.open(outname_sim,ios::out);
	ofstream outputfile_hamming;
	if (DEMO==0) {
		string outname_hamming = outname+"-hamming.txt";
		outputfile_hamming.open(outname_hamming,ios::out);
	}
	unsigned int * query = query_all;
	float * query_feature = (float*)query_mat.data;
	runtimes[1]=(float)(get_wall_time() - t[1]);

	// Parallelize this for batch processing.
	// Beware of feature.data manipulation
	for  (int k=0;k<query_num;k++)
	{
		std::cout <<  "Looking for similar images of query #" << k+1 << std::endl;
		//hashing
		unsigned int * hash_data= (unsigned int*)itq.data;
		t[1]=get_wall_time();
		for (int i=0;i<data_num;i++)
		{
			hamming[i] = mypair(0,i);
			for (int j=0;j<int_num;j++)
			{
				unsigned int xnor = query[j]^hash_data[j];
				hamming[i].first += NumberOfSetBits(xnor);
			}
			hash_data += int_num;
		}
		std::sort(hamming.begin(),hamming.end(),comparator);
		query += int_num;
		runtimes[2]+=(float)(get_wall_time() - t[1]);

		//read needed feature
		t[1]=get_wall_time();
		char* feature_p = (char*)feature.data;
		int i = 0;
		int status = 0;
		read_size = sizeof(float)*feature_dim;
		for (;i<top_feature;i++)
		{
			status = get_onefeatcomp(hamming[i].second,read_size,accum,read_in_compfeatures,read_in_compidx,feature_p);
			//status = get_onefeat(hamming[i].second,read_size,accum,read_in_features,feature_p);
			if (status==-1) {
        		std::cout << "Could not get feature " << hamming[i].second << ". Exiting." << std::endl;
        		// TODO: We should clean here
        		return -1;
    		}
			feature_p +=read_size;
		}
		cout<<"Biggest hamming distance is: "<<hamming[i].first<<endl;
		runtimes[0]+=(float)(get_wall_time() - t[1]);

		//post ranking
		t[1]=get_wall_time();
		float* data_feature;
		if (norm)
		{
			for (int i=0;i<top_feature;i++)
			{
				postrank[i]= mypairf(1.0f,hamming[i].second);
				if (query_num>read_thres)
					data_feature = (float*)feature.data+feature_dim*postrank[i].second;
				else
					data_feature = (float*)feature.data+feature_dim*i;

				for (int j=0;j<feature_dim;j++)
				{
					postrank[i].first-=query_feature[j]*data_feature[j];
				}// Shouldn't we multiply by 2 to get the real euclidean distance value?...

			}
		}
		else
		{
			for (int i=0;i<top_feature;i++)
			{
				postrank[i]= mypairf(0.0f,hamming[i].second);
				if (query_num>read_thres)
					data_feature = (float*)feature.data+feature_dim*postrank[i].second;
				else
					data_feature = (float*)feature.data+feature_dim*i;

				for (int j=0;j<feature_dim;j++)
				{
					postrank[i].first+=pow(query_feature[j]-data_feature[j],2);
				}
				//postrank[i].first= sqrt(postrank[i].first);
			}
		}
		std::sort(postrank.begin(),postrank.end(),comparatorf);
		query_feature +=feature_dim;
		runtimes[3]+=(float)(get_wall_time() - t[1]);
		//cout << postrank[0].second << std::endl;
		//output
		t[1]=get_wall_time();
		for (int i=0;i<top_feature;i++) {
			outputfile << postrank[i].second << ' ';
			if (DEMO==0) {
				outputfile_hamming << postrank[i].second << ' ';
			}
		}
		for (int i=0;i<top_feature;i++) {
			outputfile << postrank[i].first << ' ';
			if (DEMO==0) {
				outputfile_hamming << hamming[i].first << ' ';
			}
		}
		outputfile << endl;
		if (DEMO==0) {
			outputfile_hamming << endl;
		}
		runtimes[4]+=(float)(get_wall_time() - t[1]);
	}

	delete[] accum;
	delete[] query_all;
	outputfile.close();
	if (DEMO==0) {
		outputfile_hamming.close();
	}
	
	for (int i = 1; i<data_nums.size();i++)
	{
		read_in_compfeatures[i]->close();
		read_in_features[i]->close();
		read_in_compidx[i]->close();
		delete read_in_compfeatures[i];
		delete read_in_features[i];
		delete read_in_compidx[i];
	}

	cout << "loading (seconds): " << runtimes[0] << std::endl;
	cout << "hashing init (seconds): " << runtimes[1] << std::endl;
	cout << "hashing (seconds): " << runtimes[2] << std::endl;
	cout << "post ranking (seconds): " << runtimes[3] << std::endl;
	cout << "output (seconds): " << runtimes[4] << std::endl;
	cout << "total time (seconds): " << (float)(get_wall_time() - t[0]) << std::endl;
	return 0;
}
