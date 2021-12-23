# contrib/tbase_subscription/Makefile

MODULE_big = tbase_subscription
OBJS = tbase_subscription.o

EXTENSION = tbase_subscription
DATA = tbase_subscription--1.0.sql \
	tbase_subscription--unpackaged--1.0.sql

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/tbase_subscription
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
