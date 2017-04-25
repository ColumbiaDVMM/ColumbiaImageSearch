INCLUDE_DIRS=/opt/local/include/
LIB_DIRS=/opt/local/lib/
CC=g++
#CC=g++-mp-5
#CC=g++-mp-4.9

INCLUDE_DIRS=/usr/local/include/
LIB_DIRS=/usr/local/lib/
#CC=/usr/local/bin/gcc

OPEN_MP=-fopenmp
OPEN_MP=

CFLAGS=-c -std=c++0x -O2
#CFLAGS=-c -std=c++11 -O2
#CFLAGS=-std=c++0x -O2
CVLIBS=-lopencv_core -lopencv_highgui -lz

INCLUDE_DIRS_CMD=$(foreach d,$(INCLUDE_DIRS),-I$d)


all: hashing hashing_obj_main hashing_update compress_feats get_precomp_feats get_precomp_hashcodes

hashing: main.o header.o iotools.o
	$(CC) main.o header.o iotools.o -o hashing $(INCLUDE_DIRS_CMD) $(CVLIBS) -L$(LIB_DIRS)

hashing_obj_main: hasher_obj.o header.o iotools.o
	$(CC) hashing_obj_main.cpp hasher_obj.o header.o iotools.o -o hashing_obj_main $(INCLUDE_DIRS_CMD) $(CVLIBS) -L$(LIB_DIRS)

hashing_update: update.o header.o 
	$(CC) update.o header.o -o hashing_update $(OPEN_MP) $(CVLIBS) -L$(LIB_DIRS)

compress_feats: compress_feats.o header.o iotools.o
	$(CC) compress_feats.o header.o iotools.o -o compress_feats $(INCLUDE_DIRS_CMD) $(CVLIBS) -L$(LIB_DIRS)

get_precomp_feats: get_precomp_feats.o iotools.o header.o
	$(CC) get_precomp_feats.o header.o iotools.o -o get_precomp_feats $(INCLUDE_DIRS_CMD) $(CVLIBS) -L$(LIB_DIRS)

get_precomp_hashcodes: get_precomp_hashcodes.o iotools.o header.o
	$(CC) get_precomp_hashcodes.o header.o iotools.o -o get_precomp_hashcodes $(INCLUDE_DIRS_CMD) $(CVLIBS) -L$(LIB_DIRS)

iotools.o: iotools.cpp iotools.h header.h
	$(CC) $(CFLAGS) iotools.cpp -o iotools.o $(INCLUDE_DIRS_CMD)

main.o: main.cpp header.h iotools.h
	$(CC) $(CFLAGS) main.cpp -o main.o $(INCLUDE_DIRS_CMD)
#$(OPEN_MP) $(CVLIBS) -L$(LIB_DIRS)

hashing_obj_main.o: hashing_obj_main.cpp hashing_obj.hpp header.h iotools.h
	$(CC) $(CFLAGS) hashing_obj_main.cpp -o hashing_obj_main.o $(INCLUDE_DIRS_CMD)

#hasher_obj.o: header.h iotools.h
hasher_obj.o: 
	$(CC) $(CFLAGS) hasher_obj.cpp -o hasher_obj.o $(INCLUDE_DIRS_CMD)

update.o: update.cpp header.h 
	$(CC) $(CFLAGS) update.cpp -o update.o $(OPEN_MP) $(INCLUDE_DIRS_CMD) $(CVLIBS) -L$(LIB_DIRS)

compress_feats.o: compress_feats.cpp header.h iotools.h
	$(CC) $(CFLAGS) compress_feats.cpp -o compress_feats.o $(INCLUDE_DIRS_CMD)

get_precomp_feats.o: get_precomp_feats.cpp header.h iotools.h
	$(CC) $(CFLAGS) get_precomp_feats.cpp -o get_precomp_feats.o $(INCLUDE_DIRS_CMD)

get_precomp_hashcodes.o: get_precomp_hashcodes.cpp header.h iotools.h
	$(CC) $(CFLAGS) get_precomp_hashcodes.cpp -o get_precomp_hashcodes.o $(INCLUDE_DIRS_CMD)

header.o: header.cpp header.h 
	$(CC) $(CFLAGS) header.cpp -o header.o

clean:
	rm -rf *o hashing get_precomp_feats compress_feats hashing_obj_main


