
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

src-console=mpack.c trace.c protocol.c engine.c
src-console-y=$(addprefix ../lib/,$(src-console)) main.c

target-console-y=$(OUT)$(CROSS_PREFIX)balboa-backend-console

dirs-y=.

all: $(target-console-y)

$(OUT)build:
	@echo "    mkdir"
	$(Q)mkdir -p $(addprefix $(OUT),$(dirs-y))
	$(Q)touch $@

$(target-console-y): Makefile $(OUT)build $(src-console-y) $(hdr-lib-y) Makefile
	$(CC) $(CFLAGS) $(src-console-y) -o $(target-console-y) $(LDFLAGS)

clean:
	rm -f $(target-console-y)
	rm -f $(OUT)build
	rmdir $(OUT)
