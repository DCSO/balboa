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

src-balboa-mock=trace.c daemon.c mpack.c engine.c
src-balboa-mock-y=$(addprefix ../lib/,$(src-balboa-mock))

hdr-balboa-mock=engine.h trace.h daemon.h
hdr-balboa-mock-y=$(addprefix ../lib/,$(hdr-balboa-mock)) mock.h

src-balboa-mock-y+=mock.c main.c
ifdef D
target-balboa-mock-y=$(OUT)$(CROSS_PREFIX)balboa-mock-debug
else
target-balboa-mock-y=$(OUT)$(CROSS_PREFIX)balboa-mock
endif

dirs-y=.

all: $(target-balboa-mock-y)

$(OUT)build:
	@echo "    mkdir"
	$(Q)mkdir -p $(addprefix $(OUT),$(dirs-y))
	$(Q)touch $@

$(target-balboa-mock-y): $(OUT)build $(src-balboa-mock-y) $(hdr-balboa-mock-y) mpack-config.h Makefile
	@mkdir -p $(OUT)
	$(CC) $(CFLAGS) $(src-balboa-mock-y) -o $(target-balboa-mock-y) $(LDFLAGS)

clean:
	rm -rf build
