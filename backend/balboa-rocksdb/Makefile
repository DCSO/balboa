ifndef OS
OS:=linux
endif

CROSS_HOST?=$(shell uname -m)
CROSS_PREFIX?=

OUT=build/$(OS)/

CFLAGS?=
CFLAGS+=-pipe -fmax-errors=3 -Wall -Wextra -s -Ofast -flto -fwhole-program -std=gnu11 -D_GNU_SOURCE -D__TRACE__
CFLAGS+=-I. -I../lib -I../vendor/github.com/ludocode/mpack

LDFLAGS?=
LDFLAGS+=-pthread -lrocksdb

ifeq ($(OS),linux)
CFLAGS+=-D__POSIX__
endif

ifdef D
CFLAGS+=-D__ASSERT__ -D__DEBUG__
else
CFLAGS+=
endif

ifdef V
CFLAGS+=-D__VERBOSE__
endif

MAKEFLAGS+=--no-print-directory

CC=$(CROSS_PREFIX)gcc

CFLAGS+=-DMPACK_HAS_CONFIG

src-balboa-rocksdb=trace.c daemon.c engine.c mpack.c
src-balboa-rocksdb-y=$(addprefix ../lib/,$(src-balboa-rocksdb))

hdr-balboa-rocksdb=protocol.h engine.h trace.h daemon.h mpack.h
hdr-balboa-rocksdb-y=$(addprefix ../lib/,$(hdr-balboa-rocksdb)) rocksdb.h

src-balboa-rocksdb-y+=rocksdb.c main.c
ifdef D
target-balboa-rocksdb-y=$(OUT)$(CROSS_PREFIX)balboa-rocksdb-debug
else
target-balboa-rocksdb-y=$(OUT)$(CROSS_PREFIX)balboa-rocksdb
endif

dirs-y=.

all: $(target-balboa-rocksdb-y)

$(OUT)build:
	@echo "    mkdir"
	$(Q)mkdir -p $(addprefix $(OUT),$(dirs-y))
	$(Q)touch $@

$(target-balboa-rocksdb-y): $(OUT)build $(src-balboa-rocksdb-y) $(hdr-balboa-rocksdb-y) mpack-config.h
	@mkdir -p $(OUT)
	$(CC) $(CFLAGS) $(src-balboa-rocksdb-y) -o $(target-balboa-rocksdb-y) $(LDFLAGS)

clean:
	rm -rf build
