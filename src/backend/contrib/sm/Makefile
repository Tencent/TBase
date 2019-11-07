#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for backend/contrib/sm
#
# IDENTIFICATION
#    src/backend/contrib/sm/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/contrib/sm
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

#compile crypt procedure specailly
CFLAGS = -O3

SRCS		= sm4.c 

OBJS		= $(SRCS:.c=.o)

include $(top_srcdir)/src/backend/common.mk
