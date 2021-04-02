#----------------------------------------------------------------------------
#
# Postgres-XC GTM common makefile
#
# Copyright(c) 2010-2012 Postgres-XC Development Group
#
# src/gtm/common/Makefile
#
#-----------------------------------------------------------------------------
top_builddir=../../..
subdir = src/gtm/common

include $(top_builddir)/src/Makefile.global

override CPPFLAGS := -I. -I$(srcdir) $(CPPFLAGS)

NAME=gtm

SO_MAJOR_VERSION= 1
SO_MINOR_VERSION= 0

LDFLAGS=-L$(top_builddir)/common -L$(top_builddir)/libpq
LIBS=-lpthread -lrt

OBJS = gtm_opt_handler.o aset.o mcxt.o gtm_utils.o elog.o assert.o stringinfo.o gtm_lock.o \
       gtm_list.o gtm_serialize.o gtm_serialize_debug.o gtm_time.o gtm_gxid.o heap.o datapump.o bloom.o syslogger.o

all:all-lib

gtm_opt_handler.o: gtm_opt_scanner.c

gtm_opt_scanner.c: gtm_opt_scanner.l
ifdef FLEX
	$(FLEX) $(FLEXFLAGS) -o'$@' $<
else
	@$(missing) flex $< $@
endif

# Shared library stuff
include $(top_srcdir)/src/Makefile.shlib

# Note that gtm_opt_scanner.c is not deleted by make clean as we want it in distribution tarballs
clean:
	rm -f $(OBJS)
	rm -f libgtm.so libgtm.so.1 libgtm.so.1.0

distclean: clean

maintainer-clean: distclean
