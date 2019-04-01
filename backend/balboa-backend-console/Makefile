ifndef OS
OS:=linux
endif

CROSS_HOST?=$(shell uname -m)
CROSS_PREFIX?=

OUT=build/$(OS)/

CFLAGS?=
CFLAGS+=-pipe -static -fmax-errors=3 -Wall -Wextra -s -Ofast -flto -fwhole-program -std=gnu11 -D_GNU_SOURCE -D__TRACE__
CFLAGS+=-I. -I../lib

LDFLAGS?=
LDFLAGS+=-pthread

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

hdr-lib=bs.h trace.h protocol.h engine.h
hdr-lib-y=$(addprefix ../lib/,$(hdr-lib))

src-console-read-y=../lib/mpack.c ../lib/trace.c ../lib/protocol.c main.c
ifdef D
target-console-read-y=$(OUT)$(CROSS_PREFIX)balboa-backend-console-debug
else
target-console-read-y=$(OUT)$(CROSS_PREFIX)balboa-backend-console
endif

dirs-y=.

all: $(target-console-read-y)

$(OUT)build:
	@echo "    mkdir"
	$(Q)mkdir -p $(addprefix $(OUT),$(dirs-y))
	$(Q)touch $@

$(target-console-read-y): Makefile $(OUT)build $(src-console-read-y) $(hdr-lib-y) mpack-config.h
	@mkdir -p $(OUT)
	$(CC) $(CFLAGS) $(src-console-read-y) -o $(target-console-read-y) $(LDFLAGS)

clean:
	rm -rf build
