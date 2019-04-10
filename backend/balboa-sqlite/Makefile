
CROSS_HOST?=$(shell uname -m)
CROSS_PREFIX?=
CCOMPILER?=gcc

OUT=build/

SQLITE=sqlite-amalgamation-3270200
SQLITEFLAGS=-DSQLITE_THREADSAFE=1 -DSQLITE_ENABLE_FTS5
#-DSQLITE_ENABLE_RBU

CFLAGS?=
CFLAGS+=-pipe -s -O3 -flto
ifeq ($(CCOMPILER),gcc)
CFLAGS+=-static -fwhole-program -fmax-errors=3 -D__GCC__
endif
ifeq ($(CCOMPILER),clang)
CFLAGS+=-static -D__CLANG__
endif
CFLAGS+=-std=c11 -Wall -Wextra -D_GNU_SOURCE -D__TRACE__ -DNDEBUG
CFLAGS+=-I. -I../lib -I$(SQLITE)
CFLAGS+=-DMPACK_HAS_CONFIG

LDFLAGS?=
LDFLAGS+=-ldl -pthread

MAKEFLAGS+=--no-print-directory

CC=$(CROSS_PREFIX)$(CCOMPILER)

hdr-sqlite=protocol.h engine.h trace.h daemon.h
hdr-sqlite-y=$(addprefix ../lib/,$(hdr-sqlite)) $(SQLITE)/sqlite3.h sqlite-impl.h

src-sqlite=trace.c daemon.c mpack.c protocol.c engine.c
src-sqlite-y=$(addprefix ../lib/,$(src-sqlite)) $(SQLITE)/sqlite3.c
src-sqlite-y+=sqlite-impl.c main.c

target-sqlite-y=$(OUT)$(CROSS_PREFIX)balboa-sqlite

dirs-y=.

all: $(target-sqlite-y)

$(OUT)build:
	@echo "    mkdir"
	$(Q)mkdir -p $(addprefix $(OUT),$(dirs-y))
	$(Q)touch $@

$(target-sqlite-y): $(OUT)build $(src-sqlite-y) $(hdr-sqlite-y) mpack-config.h Makefile
	@mkdir -p $(OUT)
	$(CC) $(CFLAGS) $(src-sqlite-y) -o $(target-sqlite-y) $(LDFLAGS)

clean:
	rm $(target-sqlite-y)
	rm $(OUT)build
	rmdir $(OUT)
