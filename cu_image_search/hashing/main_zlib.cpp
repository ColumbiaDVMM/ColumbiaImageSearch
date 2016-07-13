#include "header.h"
//#include <omp.h>
//#include <vl/generic.h>

#include <opencv2/opencv.hpp>

//#include <math.h>
#include <fstream>


using namespace std;
using namespace cv;
#include <stdio.h>
#include <string.h>  // for strlen
#include <assert.h>
#include "zlib.h"

// adapted from: http://stackoverflow.com/questions/7540259/deflate-and-inflate-zlib-h-in-c
int main(int argc, char* argv[])
{   

ifstream read_in("../../memex/feature_norm",ios::in|ios::binary);
if (!read_in.is_open())
	{
		std::cout << "Cannot load the feature file!" << std::endl;
		return -1;
	}
	char * feature;
int feature_dim=4096;

int data_num = 20000;
size_t 		read_size=0;
		read_size = sizeof(float)*data_num*feature_dim;
feature = new char[read_size];
		read_in.read(feature, read_size);
		read_in.close();    // original string len = 36

char * a = feature;

    // placeholder for the compressed (deflated) version of "a" 
    char* b=new char[read_size];

    // placeholder for the UNcompressed (inflated) version of "b"
    char * c=new char[read_size];

    printf("Uncompressed size is: %lu\n", read_size);
//    printf("Uncompressed string is: %s\n", a);


    printf("\n----------\n\n");
int fsize = read_size/data_num; 
int *b_outs= new int[data_num];
int b_out = 0;
for (int i=0;i<data_num;i++)
{


    // STEP 1.
    // deflate a into b. (that is, compress a into b)
    
    // zlib struct
    z_stream defstream;
    defstream.zalloc = Z_NULL;
    defstream.zfree = Z_NULL;
    defstream.opaque = Z_NULL;
    // setup "a" as the input and "b" as the compressed output
    defstream.avail_in = (uInt)fsize; // size of input, string + terminator
    defstream.next_in = (Bytef *)a+i*fsize; // input char array
    defstream.avail_out = (uInt)fsize; // size of output
    defstream.next_out = (Bytef *)b+i*fsize; // output char array
    
    // the actual compression work.
    deflateInit(&defstream, Z_BEST_COMPRESSION);
    deflate(&defstream, Z_FINISH);
    deflateEnd(&defstream);
    b_outs[i]=defstream.total_out;
    b_out+=defstream.total_out;
}     
    // This is one way of getting the size of the output
printf("Compressed size is: %d\n", b_out);
  //  printf("Compressed string is: %s\n", defstream.total_out, b);
//    printf("Compressed size is: %lu\n", strlen(b));
  //  printf("Compressed string is: %s\n", b);
    

    printf("\n----------\n\n");
int c_out = 0;
double	t = get_wall_time(); // Start Time

for (int i=0;i<data_num;i++)
{

    // STEP 2.
    // inflate b into c
    // zlib struct
    z_stream infstream;
    infstream.zalloc = Z_NULL;
    infstream.zfree = Z_NULL;
    infstream.opaque = Z_NULL;
    // setup "b" as the input and "c" as the compressed output
    infstream.avail_in = (uInt)(b_outs[i]); // size of input
    infstream.next_in = (Bytef *)b+i*fsize; // input char array
    infstream.avail_out = (uInt)fsize; // size of output
    infstream.next_out = (Bytef *)c+i*fsize; // output char array
     
    // the actual DE-compression work.
    inflateInit(&infstream);
    inflate(&infstream, Z_NO_FLUSH);
    inflateEnd(&infstream);
c_out+=infstream.total_out;
     }
cout << "total time (seconds): " << (float)(get_wall_time() - t) << std::endl;
printf("Uncompressed size is: %d\n", c_out);
//printf("Uncompressed string is: %.*s\n", infstream.total_out, c);
//    printf("Uncompressed size is: %lu\n", strlen(c));
 //   printf("Uncompressed string is: %s\n", c);
for (int i=0;i<read_size;i++)
{
assert(a[i]==c[i]);
//cout<<a[i]<<c[i]<<endl; 
}   

    // make sure uncompressed is exactly equal to original.
//    assert(strcmp(a,c)==0);

    return 0;
}
