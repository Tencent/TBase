# contrib/pg_clean/Makefile

MODULE_big = pg_clean
OBJS = pg_clean.o

EXTENSION = pg_clean
DATA = pg_clean--1.0.sql pg_clean--unpackaged--1.0.sql

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_clean
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif