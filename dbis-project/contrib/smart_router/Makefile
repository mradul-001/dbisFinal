MODULE_big = smart_router
OBJS = smart_router.o
EXTENSION = smart_router
DATA = smart_router--1.0.sql
PGFILEDESC = "Smart DB Router and Cacher"

# Header search path
PG_CPPFLAGS = -I$(libpq_srcdir)

SHLIB_LINK_INTERNAL = $(libpq)

# In-tree build configuration
subdir = contrib/smart_router
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk