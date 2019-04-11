
MAKEFLAGS+=--no-print-directory

all: rocksdb console mock

rocksdb:
	${MAKE} -C balboa-rocksdb

console:
	${MAKE} -C balboa-backend-console

mock:
	${MAKE} -C balboa-mock

sqlite:
	${MAKE} -C balboa-sqlite

style:
	clang-format -i \
		lib/protocol.{c,h} lib/engine.{c,h} lib/daemon.{c,h} lib/alloc.h lib/trace.{c,h} \
		balboa-rocksdb/rocksdb-impl.{c,h} balboa-rocksdb/main.c \
		balboa-mock/mock-impl.{c,h} balboa-mock/main.c \
		balboa-sqlite/sqlite-impl.{c,h} balboa-sqlite/main.c

clean:
	${MAKE} -C balboa-mock clean
	${MAKE} -C balboa-backend-console clean
	${MAKE} -C balboa-rocksdb clean
	${MAKE} -C balboa-sqlite clean
