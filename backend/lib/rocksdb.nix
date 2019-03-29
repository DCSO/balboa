{ stdenv, fetchFromGitHub, lib, bzip2, cmake, lz4, snappy, zlib, zstd, enableLite ? false }:

stdenv.mkDerivation rec {
  name = "rocksdb";
  version = "5.18.3";

  src = fetchFromGitHub {
    owner = "facebook";
    repo = name;
    rev = "v${version}";
    sha256 = "1v2slmmr1dsgf8z0qcfg1y9x1al96859rg48b66p9nsawczd5zv9";
  };

  nativeBuildInputs = [ cmake ];
  buildInputs = [ bzip2 lz4 snappy zlib zstd ];

  cmakeFlags = [
    "-DCMAKE_BUILD_TYPE=Release"
    "-DPORTABLE=1"
    "-DWITH_JEMALLOC=0"
    "-DWITH_JNI=0"
    "-DWITH_TESTS=0"
    "-DWITH_TOOLS=0"
    "-DWITH_BZ2=1"
    "-DWITH_LZ4=1"
    "-DWITH_SNAPPY=1"
    "-DWITH_ZLIB=1"
    "-DWITH_ZSTD=1"
    (lib.optional
        (stdenv.hostPlatform.system == "i686-linux"
         || stdenv.hostPlatform.system == "x86_64-linux")
        "-DFORCE_SSE42=1")
    (lib.optional enableLite "-DROCKSDB_LITE=1")
  ];

  meta = with stdenv.lib; {
    homepage = https://rocksdb.org;
    description = "A library that provides an embeddable, persistent key-value store for fast storage";
    license = licenses.bsd3;
    maintainers = with maintainers; [ adev magenbluten ];
  };
}
