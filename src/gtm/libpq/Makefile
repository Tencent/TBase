#----------------------------------------------------------------------------
#
# Postgres-XC GTM libpq makefile
#
# Copyright(c) 2010-2012 Postgres-XC Development Group
#
# src/gtm/libpq/Makefile
#
#-----------------------------------------------------------------------------
top_builddir=../../..
include $(top_builddir)/src/Makefile.global
subdir = src/gtm/libpq

NAME=pqcomm
SO_MAJOR_VERSION= 1
SO_MINOR_VERSION= 0

OBJS=ip.o pqcomm.o pqformat.o strlcpy.o pqsignal.o

all:all-lib

include $(top_srcdir)/src/Makefile.shlib

clean:
	rm -f $(OBJS)
	rm -f libpqcomm.so libpqcomm.so.1 libpqcomm.so.1.0 

distclean: clean

maintainer-clean: distclean
