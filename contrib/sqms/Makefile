MODULE_big = sqms

OBJS = \
$(WIN32RES) \
src/collect/format.o \
src/collect/epxr_utils.o \
src/collect/stat_collect.o \
src/collect/stat_format.o \
src/discovery/discovery.o \
src/excavate/ExcavateProvider.o \

SHLIB_LINK_INTERNAL = $(libpq) -lstdc++ 
CFLAGS += -std=c11 -I/home/yyk/postgresql-13.10/contrib/sqms/src/include 
PG_CPPFLAGS = -std=c++11 -fPIC -I$(libpq_srcdir) -lstdc++ -fno-rtti -I/home/yyk/postgresql-13.10/contrib/sqms/src/include

# pg extend config 
EXTENSION = sqms
DATA = sqms--1.0.sql
PGFILEDESC = "sqms - slow query manager system"

REGRESS_OPTS = --temp-config $(top_srcdir)/contrib/sqms/sqms.conf
REGRESS = sqms 

#we need separate c & c++ source files,use differnet complier
CXX = g++
CC = gcc

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

#support make clean
.PHONY:clean
clean:
	rm -rf /home/yyk/postgresql-13.10/contrib/sqms/*.o *.so
	rm -rf /home/yyk/postgresql-13.10/contrib/sqms/src/collect/*.o *.so
	rm -rf /home/yyk/postgresql-13.10/contrib/sqms/src/discovery/*.o *.so
	rm -rf /home/yyk/postgresql-13.10/contrib/sqms/src/excavate/*.o *.so
	rm -rf /home/yyk/postgresql-13.10/contrib/sqms/src/storage/*.o *.so
