#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for src/backend/postmaster
#
# IDENTIFICATION
#    src/backend/postmaster/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/postmaster
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

OBJS = auditlogger.o autovacuum.o bgworker.o bgwriter.o checkpointer.o clustermon.o \
	fork_process.o pgarch.o pgstat.o postmaster.o startup.o syslogger.o walwriter.o clean2pc.o

include $(top_srcdir)/src/backend/common.mk
