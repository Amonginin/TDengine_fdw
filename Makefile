# 要构建的模块名称
MODULE_big = tdengine_fdw
# 构建模块所需的目标文件列表
# TODO:
OBJS = option.o slvars.o deparse.o influxdb_query.o influxdb_fdw.o

# ifndef GO_CLIENT
# ifndef CXX_CLIENT
# GO_CLIENT = 1
# endif
# endif

# ifdef CXX_CLIENT
# remove C interface of GO client
$(shell rm -rf ./_obj)
$(shell rm -f ./query.h)

# HowardHinnant date library source dir
DATE_LIB = -I./deps/date/include

OBJS += query.o tz.o connection.o
PG_CPPFLAGS += -DCXX_CLIENT $(DATE_LIB)
SHLIB_LINK = -lm -lstdc++ -lInfluxDB

# query.cpp requires C++ 17.
# 强制 PG_CXXFLAGS 使用 C++ 17 标准
override PG_CXXFLAGS += -std=c++17

# override PG_CXXFLAGS and PG_CFLAGS
ifdef CCFLAGS
	override PG_CXXFLAGS += $(CCFLAGS)
	override PG_CFLAGS += $(CCFLAGS)
endif #!CCFLAGS

# 如果没有启用 C++ 客户端，则启用 Go 客户端
# else
# PG_CPPFLAGS += -DGO_CLIENT
# OBJS += query.a
# endif #!CXX_CLIENT

# 指定要构建的扩展名称
EXTENSION = tdengine_fdw
# TODO:扩展所需的数据文件列表
DATA = influxdb_fdw--1.0.sql influxdb_fdw--1.1.sql influxdb_fdw--1.1--1.2.sql influxdb_fdw--1.2.sql influxdb_fdw--1.3.sql

# TODO:要执行的回归测试名称，用于验证扩展的功能
REGRESS = option aggregate influxdb_fdw selectfunc extra/join extra/limit extra/aggregates extra/insert extra/prepare extra/select_having extra/select extra/influxdb_fdw_post schemaless/aggregate schemaless/influxdb_fdw schemaless/selectfunc schemaless/schemaless schemaless/extra/join schemaless/extra/limit schemaless/extra/aggregates schemaless/extra/prepare schemaless/extra/select_having schemaless/extra/insert schemaless/extra/select schemaless/extra/influxdb_fdw_post schemaless/add_fields schemaless/add_tags schemaless/add_multi_key 

UNAME = uname
OS := $(shell $(UNAME))
ifeq ($(OS), Darwin)
DLSUFFIX = .dylib
else
DLSUFFIX = .so
endif

# 如果定义了 USE_PGXS，
# 则使用 pg_config 工具获取 PostgreSQL 的 PGXS（PostgreSQL 扩展构建系统）路径，
# 并包含该文件
ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
ifndef MAJORVERSION
MAJORVERSION := $(basename $(VERSION))
endif
# 如果没有定义 USE_PGXS，则设置项目的子目录和顶级构建目录，
# 并包含 PostgreSQL 的全局 Makefile 和贡献模块的 Makefile
else
subdir = contrib/influxdb_fdw
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
ifndef MAJORVERSION
MAJORVERSION := $(basename $(VERSION))
endif
endif

# PGSQL版本检查
ifeq (,$(findstring $(MAJORVERSION), 13 14 15 16 17))
$(error PostgreSQL 13, 14, 15, 16 or 17 is required to compile this extension)
endif

ifdef REGRESS_PREFIX
REGRESS_PREFIX_SUB = $(REGRESS_PREFIX)
else
REGRESS_PREFIX_SUB = $(VERSION)
endif

# 回归测试前缀配置
REGRESS := $(addprefix $(REGRESS_PREFIX_SUB)/,$(REGRESS))
$(shell mkdir -p results/$(REGRESS_PREFIX_SUB)/extra)
$(shell mkdir -p results/$(REGRESS_PREFIX_SUB)/schemaless)
$(shell mkdir -p results/$(REGRESS_PREFIX_SUB)/schemaless/extra)

# Go 客户端构建规则
# ifdef GO_CLIENT
# query.a: query.go
# 	go build -buildmode=c-archive query.go
# $(OBJS):  _obj/_cgo_export.h
# _obj/_cgo_export.h: query.go
# 	go tool cgo query.go
# endif	#!GO_CLIENT

# ifdef CXX_CLIENT
# PostgreSQL uses link time optimization option which may break compilation
# (this happens on travis-ci). Redefine COMPILE.cxx.bc without this option.
COMPILE.cxx.bc = $(CLANG) -xc++ -Wno-ignored-attributes $(BITCODE_CXXFLAGS) $(CPPFLAGS) -emit-llvm -c

# A hurdle to use common compiler flags when building bytecode from C++
# files. should be not unnecessary, but src/Makefile.global omits passing those
# flags for an unnknown reason.
%.bc : %.cpp
	$(COMPILE.cxx.bc) $(CXXFLAGS) $(CPPFLAGS)  -o $@ $<

# Using OS timezone data base for date library
DATE_DEF = -DUSE_OS_TZDB
CURL_LIB = -lcurl

tz.o: deps/date/src/tz.cpp
	g++ -fPIC $(PG_CXXFLAGS) $(CXXFLAGS) $(CPPFLAGS) $(DATE_LIB) -I. $(CURL_LIB) $(DATE_DEF) -c $<

.PHONY: clean
clean: deps_clean
# clean deps object
deps_clean:
	rm -f ./tz.o
# endif #!CXX_CLIENT
