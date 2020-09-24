#----------------------------------------------------------------------------
#
# Postgres-XC GTM main Makefile
#
# Copyright(c) 2010-2012 Postgres-XC Development Group
#
# src/gtm/main/Makefile
#
#-----------------------------------------------------------------------------
top_builddir=../../..
include $(top_builddir)/src/Makefile.global
subdir = src/gtm/main

ifneq ($(PORTNAME), win32)
override CFLAGS += $(PTHREAD_CFLAGS)
endif

OBJS=main.o gtm_thread.o gtm_txn.o gtm_seq.o gtm_snap.o gtm_standby.o gtm_opt.o gtm_backup.o gtm_store.o gtm_xlog.o gtm_stat.o gtm_stat_error.o

OTHERS= ../libpq/libpqcomm.a ../path/libgtmpath.a ../recovery/libgtmrecovery.a ../client/libgtmclient.a ../common/libgtm.a ../../port/libpgport.a

gtm:$(OBJS)
	$(CC) $(CFLAGS) $(LDFLAGS) $(LIBS) $^ $(OTHERS) -o gtm

all:gtm

clean:
	rm -f $(OBJS)
	rm -f gtm

distclean: clean

maintainer-clean: distclean
