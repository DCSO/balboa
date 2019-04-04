
CROSS_HOST?=$(shell uname -m)
CROSS_PREFIX?=
CCOMPILER?=gcc

OUT=build/

CFLAGS?=
CFLAGS+=-pipe -static -s -Ofast -flto

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
LDFLAGS+=-pthread

MAKEFLAGS+=--no-print-directory

CC=$(CROSS_PREFIX)$(CCOMPILER)

hdr-lib=bs.h trace.h protocol.h engine.h mpack-config.h
hdr-lib-y=$(addprefix ../lib/,$(hdr-lib))

src-console-read-y=../lib/mpack.c ../lib/trace.c ../lib/protocol.c main.c

target-console-read-y=$(OUT)$(CROSS_PREFIX)balboa-backend-console

dirs-y=.

all: $(target-console-read-y)

$(OUT)build:
	@echo "    mkdir"
	$(Q)mkdir -p $(addprefix $(OUT),$(dirs-y))
	$(Q)touch $@

$(target-console-read-y): Makefile $(OUT)build $(src-console-read-y) $(hdr-lib-y) Makefile
	$(CC) $(CFLAGS) $(src-console-read-y) -o $(target-console-read-y) $(LDFLAGS)

clean:
	rm $(target-console-read-y)
	rm $(OUT)build
	rmdir $(OUT)
