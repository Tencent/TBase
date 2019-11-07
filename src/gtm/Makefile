#----------------------------------------------------------------------------
#
# Postgres-XC GTM makefile
#
# Copyright(c) 2010-2012 Postgres-XC Development Group
#
# src/gtm/Makefile
#
#-----------------------------------------------------------------------------
PGFILEDESC = "gtm - Global Transaction Manager for Postgres-XC"
subdir = src/gtm
top_builddir = ../..
include $(top_builddir)/src/Makefile.global

WANTED_DIRS=common path libpq client recovery main proxy gtm_ctl

# There are interdependencies between main and proxy, so
# don't attempt parallel make here.
.NOTPARALLEL:

all:
	@for dir in $(WANTED_DIRS); do \
        $(MAKE) -C $$dir $@ || exit; \
    done

clobber:
	@for dir in $(WANTED_DIRS); do \
        $(MAKE) -C $$dir $@ || exit; \
    done

clean:
	@for dir in $(WANTED_DIRS); do \
        $(MAKE) -C $$dir $@ || exit; \
    done

distclean: clean

maintainer-clean: distclean

install: all installdirs
	$(INSTALL_PROGRAM) main/gtm$(X) '$(DESTDIR)$(bindir)/gtm$(X)'
	$(INSTALL_PROGRAM) gtm_ctl/gtm_ctl$(X) '$(DESTDIR)$(bindir)/gtm_ctl$(X)'
	$(INSTALL_PROGRAM) proxy/gtm_proxy$(X) '$(DESTDIR)$(bindir)/gtm_proxy$(X)'
	$(INSTALL_DATA) $(srcdir)/main/gtm.conf.sample '$(DESTDIR)$(datadir)/gtm.conf.sample'
	$(INSTALL_DATA) $(srcdir)/proxy/gtm_proxy.conf.sample '$(DESTDIR)$(datadir)/gtm_proxy.conf.sample'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)'
	$(MKDIR_P) '$(DESTDIR)$(datadir)'

uninstall:
	rm -f $(DESTDIR)$(bindir)/gtm$(X)
	rm -f $(DESTDIR)$(bindir)/gtm_ctl$(X)
	rm -f $(DESTDIR)$(bindir)/gtm_proxy$(X)
	rm -f $(DESTDIR)$(datadir)/gtm.conf.sample
	rm -f $(DESTDIR)$(datadir)/gtm_proxy.conf.sample
