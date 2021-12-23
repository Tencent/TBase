# contrib/pg_unlock/Makefile

MODULE_big = pg_unlock
OBJS = pg_unlock.o

EXTENSION = pg_unlock
DATA = pg_unlock--1.0.sql pg_unlock--unpackaged--1.0.sql

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_unlock
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif