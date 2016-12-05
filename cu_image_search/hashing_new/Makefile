# remote
INCLUDE_DIRS=/opt/local/include/ ./src/
LIB_DIRS=/opt/local/lib/
CC=g++

# # local
# CC=g++-mp-4.9
# INCLUDE_DIRS=/usr/local/include/ ./src/
# LIB_DIRS=/usr/local/lib/

OPEN_MP=-fopenmp
#OPEN_MP=

CFLAGS=-c 
CXXFLAGS=-std=c++0x -O2
CVLIBS=-lopencv_core -lopencv_highgui -lz

INCLUDE_DIRS_CMD=$(foreach d,$(INCLUDE_DIRS),-I$d)

SRCDIR := src/
TOOLSSRCDIR := tools/
EXECDIR := ./
OBJDIR := obj/

MKDIR_P = mkdir -p

SRC := $(shell find $(SRCDIR) -name "*.cpp")
MAIN_SRC := $(shell find $(TOOLSSRCDIR) -name "*.cpp")
OBJ := $(SRC:$(SRCDIR)%.cpp=%)
EXEC := $(MAIN_SRC:$(TOOLSSRCDIR)%.cpp=$(EXECDIR)%)
DEP := $(OBJ:$(OBJDIR)%.o=%.d)

.PHONY: all clean directories

all: directories $(foreach d,$(EXEC),$d)

directories: 
	${MKDIR_P} ${OBJDIR}

define EXEC_RULE
$(1): $(foreach obj,$(OBJ),$(obj).o)
	$(CC) $(CXXFLAGS) $(TOOLSSRCDIR)$(1).cpp $(foreach obj,$(OBJ),$(OBJDIR)$(obj).o) -o $(1) $(INCLUDE_DIRS_CMD) $(OPEN_MP) $(CVLIBS) -L$(LIB_DIRS)
endef	

# build with -fPIC for shared library build with SWIG
define OBJ_RULE
$(1).o: $(SRCDIR)$(1).cpp
	$(CC) $(CXXFLAGS) $(CFLAGS) $(SRCDIR)$(1).cpp -o $(OBJDIR)$(1).o $(OPEN_MP) $(INCLUDE_DIRS_CMD)
	$(CC) $(CXXFLAGS) $(CFLAGS) -fPIC $(SRCDIR)$(1).cpp -o $(OBJDIR)$(1)_fpic.o $(OPEN_MP) $(INCLUDE_DIRS_CMD)
endef

$(foreach obj,$(OBJ),$(eval $(call OBJ_RULE,$(obj))))
$(foreach exe,$(EXEC),$(eval $(call EXEC_RULE,$(exe))))

clean:
	rm -rf *o $(EXEC) ${OBJDIR}


