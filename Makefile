MODULE_big = sqms
OBJS = \
    $(WIN32RES) \
    src/common/config.o \
    src/common/bloom_filter/bloom_filter.o \
    src/common/codec.o \
    src/common/hash_util.o \
    src/common/util.o \
    src/include/collect/format.pb-c.o \
    src/collect/format.o \
    src/collect/epxr_utils.o \
    src/collect/stat_collect.o \
    src/collect/stat_format.o \
    src/discovery/discovery.o \
    src/excavate/ExcavateProvider.o \
    src/storage/RedisPlanStatProvider.o \
    src/collect/level_mgr.o \
    src/collect/node_mgr.o \
    src/discovery/query_index_node.o \
    src/discovery/query_index.o \
    src/discovery/sm_level_mgr.o \
    src/discovery/inverted_index.o \
    src/collect/explain_slow.o \

CURRENT_DIR := $(shell pwd)
$(info CURRENT_DIR = $(CURRENT_DIR))

SHLIB_LINK_INTERNAL = $(libpq) -lstdc++ -lprotobuf -lprotobuf-c -L/usr/local/lib -lhiredis -luv -lstdc++ -L/usr/lib/x86_64-linux-gnu -ltbb 
CFLAGS += -std=c11  -I/usr/local/include 
PG_CPPFLAGS = -std=c++17 -fPIC -I$(libpq_srcdir) -I$(CURRENT_DIR)/src/include

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
