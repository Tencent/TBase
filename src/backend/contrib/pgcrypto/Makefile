#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for access/common
#
# IDENTIFICATION
#    src/backend/access/common/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/contrib/pgcrypto
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

#compile crypt procedure specailly
CFLAGS = -O3

INT_SRCS = md5.c sha1.c sha2.c internal.c internal-sha2.c blf.c rijndael.c \
		fortuna.c random.c pgp-mpi-internal.c imath.c
INT_TESTS = sha2

OSSL_SRCS = openssl.c pgp-mpi-openssl.c
OSSL_TESTS = sha2 des 3des cast5

ZLIB_TST = pgp-compression
ZLIB_OFF_TST = pgp-zlib-DISABLED
#no care if configure --with-openssl or not
CF_SRCS = $(INT_SRCS)
CF_TESTS = $(INT_TESTS)
#CF_SRCS = $(if $(subst no,,$(with_openssl)), $(OSSL_SRCS), $(INT_SRCS))
#CF_TESTS = $(if $(subst no,,$(with_openssl)), $(OSSL_TESTS), $(INT_TESTS))
CF_PGP_TESTS = $(if $(subst no,,$(with_zlib)), $(ZLIB_TST), $(ZLIB_OFF_TST))

SRCS		= pgcrypto.c px.c px-hmac.c px-crypt.c \
		crypt-gensalt.c crypt-blowfish.c crypt-des.c \
		crypt-md5.c $(CF_SRCS) \
		mbuf.c pgp.c pgp-armor.c pgp-cfb.c pgp-compress.c \
		pgp-decrypt.c pgp-encrypt.c pgp-info.c pgp-mpi.c \
		pgp-pubdec.c pgp-pubenc.c pgp-pubkey.c pgp-s2k.c \
		pgp-pgsql.c

OBJS		= $(SRCS:.c=.o)

include $(top_srcdir)/src/backend/common.mk
