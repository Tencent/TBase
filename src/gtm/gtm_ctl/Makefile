#----------------------------------------------------------------------------
#
# Postgres-XC GTM gtm_ctl makefile
#
# Copyright(c) 2010-2012 Postgres-XC Development Group
#
# src/gtm/gtm_ctl/Makefile
#
#-----------------------------------------------------------------------------
top_builddir=../../..
include $(top_builddir)/src/Makefile.global
subdir = src/gtm/gtm_ctl

OBJS=gtm_ctl.o

OTHERS= ../client/libgtmclient.a ../common/libgtm.a  ../path/libgtmpath.a   ../../port/libpgport.a ../libpq/libpqcomm.a -lpthread
gtm_ctl:$(OBJS)
	$(CC) $(CFLAGS) $(LDFLAGS) $(LIBS) $^ $(OTHERS)  -o gtm_ctl

all:gtm_ctl

clean:
	rm -f $(OBJS)
	rm -f gtm_ctl

distclean: clean

maintainer-clean: distclean
