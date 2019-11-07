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
subdir = src/gtm/xlog_test

ifneq ($(PORTNAME), win32)
override CFLAGS += $(PTHREAD_CFLAGS)
endif

OBJS=xlog_test.o ../main/gtm_thread.o ../main/gtm_txn.o ../main/gtm_seq.o ../main/gtm_snap.o ../main/gtm_standby.o ../main/gtm_opt.o ../main/gtm_backup.o ../main/gtm_store.o ../main/gtm_xlog.o
ROBJS=xlog_reader.o ../main/gtm_thread.o ../main/gtm_txn.o ../main/gtm_seq.o ../main/gtm_snap.o ../main/gtm_standby.o ../main/gtm_opt.o ../main/gtm_backup.o ../main/gtm_store.o ../main/gtm_xlog.o

OTHERS= ../libpq/libpqcomm.a ../path/libgtmpath.a ../recovery/libgtmrecovery.a ../client/libgtmclient.a ../common/libgtm.a ../../port/libpgport.a

xlog_test:$(OBJS) 
	$(CC) $(CFLAGS) $(LDFLAGS) $(LIBS) $^ $(OTHERS) -o xlog_test
xlog_reader:$(ROBJS) 
	$(CC) $(CFLAGS) $(LDFLAGS) $(LIBS) $^ $(OTHERS) -o xlog_reader

all:xlog_test xlog_reader

clean:
	rm -f $(OBJS)
	rm -f gtm

distclean: clean

maintainer-clean: distclean
