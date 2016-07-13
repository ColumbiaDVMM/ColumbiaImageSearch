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