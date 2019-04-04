
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

hdr-balboa-mock=engine.h trace.h daemon.h
hdr-balboa-mock-y=$(addprefix ../lib/,$(hdr-balboa-mock)) mock.h mpack-config.h

src-balboa-mock=trace.c daemon.c protocol.c mpack.c engine.c
src-balboa-mock-y=$(addprefix ../lib/,$(src-balboa-mock))
src-balboa-mock-y+=mock.c main.c

target-balboa-mock-y=$(OUT)$(CROSS_PREFIX)balboa-mock

dirs-y=.

all: $(target-balboa-mock-y)

$(OUT)build:
	@echo "    mkdir"
	$(Q)mkdir -p $(addprefix $(OUT),$(dirs-y))
	$(Q)touch $@

$(target-balboa-mock-y): $(OUT)build $(src-balboa-mock-y) $(hdr-balboa-mock-y) Makefile
	$(CC) $(CFLAGS) $(src-balboa-mock-y) -o $(target-balboa-mock-y) $(LDFLAGS)

clean:
	rm $(target-balboa-mock-y)
	rm $(OUT)build
	rmdir $(OUT)
