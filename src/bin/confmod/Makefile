#-------------------------------------------------------------------------
#
# Makefile for src/bin/confmod
#
# Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
#
# src/bin/confmod/Makefile
#
#-------------------------------------------------------------------------

PGFILEDESC = "confmod - check input conf file to mod or del guc option"
PGAPPICON = win32

subdir = src/bin/confmod
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

OBJS= confmod.o conf.o log.o util.o var.o stree.o

all: confmod

confmod: $(OBJS) | submake-libpgport $(PTHREAD_LIBS)
	$(CC) $(CFLAGS) $(OBJS) $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X)

install: all installdirs
	$(INSTALL_PROGRAM) confmod$(X) '$(DESTDIR)$(bindir)/confmod$(X)'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)'

uninstall:
	rm -f '$(DESTDIR)$(bindir)/confmod$(X)'

clean distclean maintainer-clean:
	rm -f confmod$(X) $(OBJS)
	rm -rf tmp_check

check:
	$(prove_check)

installcheck:
	$(prove_installcheck)
