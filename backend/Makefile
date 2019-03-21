
MAKEFLAGS+=--no-print-directory

all: rocksdb console mock

rocksdb:
	${MAKE} -C balboa-rocksdb

console:
	${MAKE} -C balboa-backend-console

mock:
	${MAKE} -C balboa-mock