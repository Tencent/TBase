MODULE_big = stormstats
OBJS	= stormstats.o

EXTENSION = stormstats
DATA = stormstats--1.0.sql stormstats--unpackaged--1.0.sql

ifdef USE_PGXS
PGXS := $(shell pg_config --pgxs)
include $(PGXS)
else
subdir = contrib/stormstats
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
