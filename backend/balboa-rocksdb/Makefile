
CROSS_HOST?=$(shell uname -m)
CROSS_PREFIX?=
CCOMPILER?=gcc

OUT=build/

CFLAGS?=
CFLAGS+=-pipe -s -Ofast -flto

ifeq ($(CCOMPILER),gcc)
CFLAGS+=-fwhole-program -fmax-errors=3 -D__GCC__
endif

ifeq ($(CCOMPILER),clang)
CFLAGS+=-D__CLANG__
endif

CFLAGS+=-std=c11 -Wall -Wextra -D_GNU_SOURCE -D__TRACE__ -DNDEBUG
CFLAGS+=-I. -I../lib
CFLAGS+=-DMPACK_HAS_CONFIG

LDFLAGS?=
LDFLAGS+=-pthread -lrocksdb

MAKEFLAGS+=--no-print-directory

CC=$(CROSS_PREFIX)$(CCOMPILER)

hdr-balboa-rocksdb=protocol.h engine.h trace.h daemon.h mpack.h mpack-config.h
hdr-balboa-rocksdb-y=$(addprefix ../lib/,$(hdr-balboa-rocksdb)) rocksdb-impl.h

src-balboa-rocksdb=trace.c daemon.c protocol.c engine.c mpack.c
src-balboa-rocksdb-y=$(addprefix ../lib/,$(src-balboa-rocksdb))
src-balboa-rocksdb-y+=rocksdb-impl.c main.c

target-balboa-rocksdb-y=$(OUT)$(CROSS_PREFIX)balboa-rocksdb

dirs-y=.

all: $(target-balboa-rocksdb-y)

$(OUT)build:
	@echo "    mkdir"
	$(Q)mkdir -p $(addprefix $(OUT),$(dirs-y))
	$(Q)touch $@

$(target-balboa-rocksdb-y): $(OUT)build $(src-balboa-rocksdb-y) $(hdr-balboa-rocksdb-y) Makefile
	$(CC) $(CFLAGS) $(src-balboa-rocksdb-y) -o $(target-balboa-rocksdb-y) $(LDFLAGS)

clean:
	rm $(target-balboa-rocksdb-y)
	rm $(OUT)build
	rmdir $(OUT)
