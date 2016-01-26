INCLUDE_DIRS=/opt/local/include/
LIB_DIRS=/opt/local/lib/
CC=g++
CFLAGS=-c -std=c++0x -O2
CVLIBS=-lopencv_core -lopencv_highgui -lz

all: hashing hashing_update compress_feats get_precomp_feats

hashing: main.o header.o iotools.o
	$(CC) main.o header.o iotools.o -o hashing -fopenmp $(CVLIBS) -L$(LIB_DIRS)

hashing_update: update.o header.o 
	$(CC) update.o header.o -o hashing_update -fopenmp $(CVLIBS) -L$(LIB_DIRS)

compress_feats: compress_feats.o header.o iotools.o
	$(CC) compress_feats.o header.o iotools.o -o compress_feats -I$(INCLUDE_DIRS) $(CVLIBS) -L$(LIB_DIRS)

get_precomp_feats: get_precomp_feats.o iotools.o header.o
	$(CC) get_precomp_feats.o header.o iotools.o -o get_precomp_feats -I$(INCLUDE_DIRS) $(CVLIBS) -L$(LIB_DIRS)

iotools.o: iotools.cpp iotools.h header.h
	$(CC) $(CFLAGS) iotools.cpp -o iotools.o

main.o: main.cpp header.h iotools.h
	$(CC) $(CFLAGS) main.cpp -o main.o -fopenmp -I$(INCLUDE_DIRS)

update.o: update.cpp header.h 
	$(CC) $(CFLAGS) update.cpp -o update.o -fopenmp -I$(INCLUDE_DIRS)

compress_feats.o: compress_feats.cpp header.h iotools.h
	$(CC) $(CFLAGS) compress_feats.cpp -o compress_feats.o -I$(INCLUDE_DIRS)

get_precomp_feats.o: get_precomp_feats.cpp header.h iotools.h
	$(CC) $(CFLAGS) get_precomp_feats.cpp -o get_precomp_feats.o -I$(INCLUDE_DIRS)

header.o: header.cpp header.h 
	$(CC) $(CFLAGS) header.cpp

clean:
	rm -rf *o hashing get_precomp_feats compress_feats


