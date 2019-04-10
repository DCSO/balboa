
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

clean:
	${MAKE} -C balboa-mock clean
	${MAKE} -C balboa-backend-console clean
	${MAKE} -C balboa-rocksdb clean
	${MAKE} -C balboa-sqlite clean
