#----------------------------------------------------------------------------
#
# Postgres-XC GTM path makefile
#
# Copyright(c) 2010-2012 Postgres-XC Development Group
#
# src/gtm/path/Makefile
#
#-----------------------------------------------------------------------------
top_builddir=../../..
include $(top_builddir)/src/Makefile.global
subdir = src/gtm/path

NAME=gtmpath
SO_MAJOR_VERSION= 1
SO_MINOR_VERSION= 0

OBJS=path.o

all:all-lib

include $(top_srcdir)/src/Makefile.shlib

clean:
	rm -f $(OBJS)
	rm -f libgtmpath.a libgtmpath.so libgtmpath.so.1 libgtmpath.so.1.0

distclean: clean

maintainer-clean: distclean

