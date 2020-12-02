# contrib/tbase_gts_tools/Makefile
MODULES = tbase_gts_tools

## 扩展名称;
EXTENSION = tbase_gts_tools

## 扩展安装的SQL文件;
DATA = tbase_gts_tools--1.0.sql

## 扩展描述;
PGFILEDESC = "tbase_gts_tools - GTS wrapper for Tbase"

### 以下为Pg构建扩展相关命令;
ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)  ## 环境变量参数加载;
else
subdir = contrib/tbase_gts_tools
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
