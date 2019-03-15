ifndef OS
OS:=linux
endif

CROSS_HOST?=$(shell uname -m)
CROSS_PREFIX?=

OUT=build/$(OS)/

CFLAGS?=
CFLAGS+=-pipe -fmax-errors=3 -Wall -Wextra -s -Ofast -flto -fwhole-program -std=gnu11 -D_GNU_SOURCE -D__TRACE__
CFLAGS+=-I. -I../lib

LDFLAGS?=
LDFLAGS+=-lpthread -lrocksdb

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

src-dump-rocksdb-y=../lib/mpack.c tpl.c main.c
ifdef D
target-dump-rocksdb-y=$(OUT)$(CROSS_PREFIX)balboa-rocksdb-v1-dump-debug
else
target-dump-rocksdb-y=$(OUT)$(CROSS_PREFIX)balboa-rocksdb-v1-dump
endif

dirs-y=.

all: $(target-dump-rocksdb-y)

$(OUT)build:
	@echo "    mkdir"
	$(Q)mkdir -p $(addprefix $(OUT),$(dirs-y))
	$(Q)touch $@

$(target-dump-rocksdb-y): Makefile $(OUT)build $(src-dump-rocksdb-y) tpl.h
	@mkdir -p $(OUT)
	$(CC) $(CFLAGS) $(src-dump-rocksdb-y) -o $(target-dump-rocksdb-y) $(LDFLAGS)

clean:
	rm -rf build
