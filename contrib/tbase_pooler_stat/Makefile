# contrib/tbase_pooler_stat/Makefile

MODULE_big = tbase_pooler_stat
OBJS = tbase_pooler_stat.o

EXTENSION = tbase_pooler_stat
DATA = tbase_pooler_stat--1.0.sql	tbase_pooler_stat--unpackaged--1.0.sql

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/tbase_pooler_stat
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
