{ stdenv, fetchFromGitHub, lib, bzip2, cmake, lz4, snappy, zstd, enableLite ? false }:

stdenv.mkDerivation rec {
  name = "rocksdb";
  version = "6.0.1";

  src = fetchFromGitHub {
    owner = "facebook";
    repo = name;
    rev = "v${version}";
    sha256 = "02jmrvr1whc54q44n7qsah9vld2x40z00kxag8s721dgzcsk37sk";
  };

  nativeBuildInputs = [ cmake ];
  buildInputs = [ bzip2 lz4 snappy zstd ];

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
    "-DWITH_ZLIB=0"
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
