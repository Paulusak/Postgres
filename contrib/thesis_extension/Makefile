# contrib/thesis_extension/Makefile

MODULES = thesis_extension

EXTENSION = thesis_extension
DATA = thesis_extension--1.0.sql

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/thesis_extension

top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
