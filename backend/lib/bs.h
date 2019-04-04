
#ifndef __BS_H
#define __BS_H

#include <ctype.h>
#include <inttypes.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>

typedef struct bytestring_source_t bytestring_source_t;
struct bytestring_source_t {
  const unsigned char* p;
  const size_t available;
  size_t index;
};

typedef struct bytestring_sink_t bytestring_sink_t;
struct bytestring_sink_t {
  unsigned char* p;
  size_t available;
  size_t index;
};

static inline uint64_t _read_w64_le( const uint8_t* p ) {
  return (
      ( ( ( uint64_t )p[0] ) << 0 ) | ( ( ( uint64_t )p[1] ) << 8 )
      | ( ( ( uint64_t )p[2] ) << 16 ) | ( ( ( uint64_t )p[3] ) << 24 )
      | ( ( ( uint64_t )p[4] ) << 32 ) | ( ( ( uint64_t )p[5] ) << 40 )
      | ( ( ( uint64_t )p[6] ) << 48 ) | ( ( ( uint64_t )p[7] ) << 56 ) );
}

static inline uint64_t _read_w64_be( uint8_t* p ) {
  return ( __builtin_bswap64( _read_w64_le( p ) ) );
}

static inline uint32_t _read_w32_le( const uint8_t* p ) {
  return (
      ( ( ( uint32_t )p[0] ) << 0 ) | ( ( ( uint32_t )p[1] ) << 8 )
      | ( ( ( uint32_t )p[2] ) << 16 ) | ( ( ( uint32_t )p[3] ) << 24 ) );
}

static inline uint32_t _read_w32_be( const uint8_t* p ) {
  return ( __builtin_bswap32( _read_w32_le( p ) ) );
}

static inline uint16_t _read_w16_le( const uint8_t* p ) {
  return ( ( ( ( uint16_t )p[0] ) << 0 ) | ( ( ( uint16_t )p[1] ) << 8 ) );
}

static inline uint16_t _read_w16_be( const uint8_t* p ) {
  return ( __builtin_bswap16( _read_w16_le( p ) ) );
}

static inline void _write_w64_le( uint8_t* p, uint64_t v ) {
  p[0] = v >> 0;
  p[1] = v >> 8;
  p[2] = v >> 16;
  p[3] = v >> 24;
  p[4] = v >> 32;
  p[5] = v >> 40;
  p[6] = v >> 48;
  p[7] = v >> 56;
}

static inline void _write_w32_le( uint8_t* p, uint32_t v ) {
  p[0] = v >> 0;
  p[1] = v >> 8;
  p[2] = v >> 16;
  p[3] = v >> 24;
}

static inline void _write_w16_le( uint8_t* p, uint16_t v ) {
  p[0] = v >> 0;
  p[1] = v >> 8;
}

static inline uint32_t bs_read_w16_le( bytestring_source_t* bs, int* ok ) {
  if( bs->index + 2 > bs->available ) {
    if( ok != NULL ) { *ok = -1; }
    return ( 0 );
  }
  uint64_t x = _read_w16_le( bs->p + bs->index );
  bs->index += 2;
  return ( x );
}

static inline uint32_t bs_read_w32_le( bytestring_source_t* bs, int* ok ) {
  if( bs->index + 4 > bs->available ) {
    if( ok != NULL ) { *ok = -1; }
    return ( 0 );
  }
  uint64_t x = _read_w32_le( bs->p + bs->index );
  bs->index += 4;
  return ( x );
}

static inline uint64_t bs_read_w64_le( bytestring_source_t* bs, int* ok ) {
  if( bs->index + 8 > bs->available ) {
    if( ok != NULL ) { *ok = -1; }
    return ( 0 );
  }
  uint64_t x = _read_w64_le( bs->p + bs->index );
  bs->index += 8;
  return ( x );
}

static inline uint64_t bs_read_w64_be( bytestring_source_t* bs, int* ok ) {
  return ( __builtin_bswap32( bs_read_w64_le( bs, ok ) ) );
}

static inline int bs_write_w16_le( bytestring_sink_t* bs, uint16_t x ) {
  if( bs->index + 2 > bs->available ) { return ( -1 ); }
  _write_w16_le( bs->p + bs->index, x );
  bs->index += 2;
  return ( x );
}

static inline int bs_write_w32_le( bytestring_sink_t* bs, uint32_t x ) {
  if( bs->index + 4 > bs->available ) { return ( -1 ); }
  _write_w32_le( bs->p + bs->index, x );
  bs->index += 4;
  return ( x );
}

static inline int bs_write_w64_le( bytestring_sink_t* bs, uint64_t x ) {
  if( bs->index + 8 > bs->available ) { return ( -1 ); }
  _write_w64_le( bs->p + bs->index, x );
  bs->index += 8;
  return ( x );
}

static inline int bs_append(
    bytestring_sink_t* bs, const uint8_t* p, size_t n ) {
  if( n > bs->available ) { return ( -1 ); }
  if( bs->index + n < bs->index ) { return ( -1 ); }
  if( bs->index + n > bs->available ) { return ( -1 ); }
  memmove( bs->p + bs->index, p, n );
  bs->index += n;
  return ( 0 );
}

static inline int bs_cat( bytestring_sink_t* bs, const char* p, size_t n ) {
  return ( bs_append( bs, ( const uint8_t* )p, n ) );
}

static inline int bs_append1( bytestring_sink_t* bs, uint8_t x ) {
  if( bs->available < 1 ) { return ( -1 ); }
  if( bs->index + 1 < bs->index ) { return ( -1 ); }
  if( bs->index + 1 > bs->available ) { return ( -1 ); }
  bs->p[bs->index] = x;
  bs->index += 1;
  return ( 0 );
}

static inline int bs_slurp( bytestring_source_t* bs, uint8_t* p, size_t n ) {
  if( n > ( bs->available - bs->index ) ) { return ( -1 ); }
  memmove( p, bs->p + bs->index, n );
  bs->index += n;
  return ( 0 );
}

static inline bytestring_sink_t bs_sink_slice0(
    const bytestring_sink_t* bs, size_t n ) {
  if( bs->available < n ) { return ( ( bytestring_sink_t ){0} ); }
  if( bs->index + n < bs->index ) { return ( ( bytestring_sink_t ){0} ); }
  if( bs->index + n > bs->available ) { return ( ( bytestring_sink_t ){0} ); }
  return ( ( bytestring_sink_t ){
      .p = bs->p + bs->index + n, .index = 0, .available = bs->available - n} );
}

#define bs_source( data, n )                      \
  ( bytestring_source_t ) {                       \
    .p = ( data ), .index = 0, .available = ( n ) \
  }
#define bs_sink( data, n )                        \
  ( bytestring_sink_t ) {                         \
    .p = ( data ), .index = 0, .available = ( n ) \
  }

static inline int bs_sink_escape( bytestring_sink_t* sink, uint8_t c ) {
  typedef struct __escape_t __escape_t;
  struct __escape_t {
    const char* r;
    unsigned int l;
  };
  static __escape_t escapes[256] = {
      [0x00] = {.r = "\\u0000", .l = 6}, [0x01] = {.r = "\\u0001", .l = 6},
      [0x02] = {.r = "\\u0002", .l = 6}, [0x03] = {.r = "\\u0003", .l = 6},
      [0x04] = {.r = "\\u0004", .l = 6}, [0x05] = {.r = "\\u0005", .l = 6},
      [0x06] = {.r = "\\u0006", .l = 6}, [0x07] = {.r = "\\u0007", .l = 6},
      [0x08] = {.r = "\\u0008", .l = 6}, [0x09] = {.r = "\\u0009", .l = 6},
      [0x0a] = {.r = "\\u000a", .l = 6}, [0x0b] = {.r = "\\u000b", .l = 6},
      [0x0c] = {.r = "\\u000c", .l = 6}, [0x0d] = {.r = "\\u000d", .l = 6},
      [0x0e] = {.r = "\\u000e", .l = 6}, [0x0f] = {.r = "\\u000f", .l = 6},
      [0x10] = {.r = "\\u0010", .l = 6}, [0x11] = {.r = "\\u0011", .l = 6},
      [0x12] = {.r = "\\u0012", .l = 6}, [0x13] = {.r = "\\u0013", .l = 6},
      [0x14] = {.r = "\\u0014", .l = 6}, [0x15] = {.r = "\\u0015", .l = 6},
      [0x16] = {.r = "\\u0016", .l = 6}, [0x17] = {.r = "\\u0017", .l = 6},
      [0x18] = {.r = "\\u0018", .l = 6}, [0x19] = {.r = "\\u0019", .l = 6},
      [0x1a] = {.r = "\\u001a", .l = 6}, [0x1b] = {.r = "\\u001b", .l = 6},
      [0x1c] = {.r = "\\u001c", .l = 6}, [0x1d] = {.r = "\\u001d", .l = 6},
      [0x1e] = {.r = "\\u001e", .l = 6}, [0x1f] = {.r = "\\u001f", .l = 6},
      [0x20] = {.r = " ", .l = 1},       [0x21] = {.r = "!", .l = 1},
      [0x22] = {.r = "\\\"", .l = 2},    [0x23] = {.r = "#", .l = 1},
      [0x24] = {.r = "$", .l = 1},       [0x25] = {.r = "%", .l = 1},
      [0x26] = {.r = "&", .l = 1},       [0x27] = {.r = "'", .l = 1},
      [0x28] = {.r = "(", .l = 1},       [0x29] = {.r = ")", .l = 1},
      [0x2a] = {.r = "*", .l = 1},       [0x2b] = {.r = "+", .l = 1},
      [0x2c] = {.r = ",", .l = 1},       [0x2d] = {.r = "-", .l = 1},
      [0x2e] = {.r = ".", .l = 1},       [0x2f] = {.r = "/", .l = 1},
      [0x30] = {.r = "0", .l = 1},       [0x31] = {.r = "1", .l = 1},
      [0x32] = {.r = "2", .l = 1},       [0x33] = {.r = "3", .l = 1},
      [0x34] = {.r = "4", .l = 1},       [0x35] = {.r = "5", .l = 1},
      [0x36] = {.r = "6", .l = 1},       [0x37] = {.r = "7", .l = 1},
      [0x38] = {.r = "8", .l = 1},       [0x39] = {.r = "9", .l = 1},
      [0x3a] = {.r = ":", .l = 1},       [0x3b] = {.r = ";", .l = 1},
      [0x3c] = {.r = "<", .l = 1},       [0x3d] = {.r = "=", .l = 1},
      [0x3e] = {.r = ">", .l = 1},       [0x3f] = {.r = "?", .l = 1},
      [0x40] = {.r = "@", .l = 1},       [0x41] = {.r = "A", .l = 1},
      [0x42] = {.r = "B", .l = 1},       [0x43] = {.r = "C", .l = 1},
      [0x44] = {.r = "D", .l = 1},       [0x45] = {.r = "E", .l = 1},
      [0x46] = {.r = "F", .l = 1},       [0x47] = {.r = "G", .l = 1},
      [0x48] = {.r = "H", .l = 1},       [0x49] = {.r = "I", .l = 1},
      [0x4a] = {.r = "J", .l = 1},       [0x4b] = {.r = "K", .l = 1},
      [0x4c] = {.r = "L", .l = 1},       [0x4d] = {.r = "M", .l = 1},
      [0x4e] = {.r = "N", .l = 1},       [0x4f] = {.r = "O", .l = 1},
      [0x50] = {.r = "P", .l = 1},       [0x51] = {.r = "Q", .l = 1},
      [0x52] = {.r = "R", .l = 1},       [0x53] = {.r = "S", .l = 1},
      [0x54] = {.r = "T", .l = 1},       [0x55] = {.r = "U", .l = 1},
      [0x56] = {.r = "V", .l = 1},       [0x57] = {.r = "W", .l = 1},
      [0x58] = {.r = "X", .l = 1},       [0x59] = {.r = "Y", .l = 1},
      [0x5a] = {.r = "Z", .l = 1},       [0x5b] = {.r = "[", .l = 1},
      [0x5c] = {.r = "\\\\", .l = 2},    [0x5d] = {.r = "]", .l = 1},
      [0x5e] = {.r = "^", .l = 1},       [0x5f] = {.r = "_", .l = 1},
      [0x60] = {.r = "`", .l = 1},       [0x61] = {.r = "a", .l = 1},
      [0x62] = {.r = "b", .l = 1},       [0x63] = {.r = "c", .l = 1},
      [0x64] = {.r = "d", .l = 1},       [0x65] = {.r = "e", .l = 1},
      [0x66] = {.r = "f", .l = 1},       [0x67] = {.r = "g", .l = 1},
      [0x68] = {.r = "h", .l = 1},       [0x69] = {.r = "i", .l = 1},
      [0x6a] = {.r = "j", .l = 1},       [0x6b] = {.r = "k", .l = 1},
      [0x6c] = {.r = "l", .l = 1},       [0x6d] = {.r = "m", .l = 1},
      [0x6e] = {.r = "n", .l = 1},       [0x6f] = {.r = "o", .l = 1},
      [0x70] = {.r = "p", .l = 1},       [0x71] = {.r = "q", .l = 1},
      [0x72] = {.r = "r", .l = 1},       [0x73] = {.r = "s", .l = 1},
      [0x74] = {.r = "t", .l = 1},       [0x75] = {.r = "u", .l = 1},
      [0x76] = {.r = "v", .l = 1},       [0x77] = {.r = "w", .l = 1},
      [0x78] = {.r = "x", .l = 1},       [0x79] = {.r = "y", .l = 1},
      [0x7a] = {.r = "z", .l = 1},       [0x7b] = {.r = "{", .l = 1},
      [0x7c] = {.r = "|", .l = 1},       [0x7d] = {.r = "}", .l = 1},
      [0x7e] = {.r = "~", .l = 1},       [0x7f] = {.r = "\\u007f", .l = 6},
      [0x80] = {.r = "\\u0080", .l = 6}, [0x81] = {.r = "\\u0081", .l = 6},
      [0x82] = {.r = "\\u0082", .l = 6}, [0x83] = {.r = "\\u0083", .l = 6},
      [0x84] = {.r = "\\u0084", .l = 6}, [0x85] = {.r = "\\u0085", .l = 6},
      [0x86] = {.r = "\\u0086", .l = 6}, [0x87] = {.r = "\\u0087", .l = 6},
      [0x88] = {.r = "\\u0088", .l = 6}, [0x89] = {.r = "\\u0089", .l = 6},
      [0x8a] = {.r = "\\u008a", .l = 6}, [0x8b] = {.r = "\\u008b", .l = 6},
      [0x8c] = {.r = "\\u008c", .l = 6}, [0x8d] = {.r = "\\u008d", .l = 6},
      [0x8e] = {.r = "\\u008e", .l = 6}, [0x8f] = {.r = "\\u008f", .l = 6},
      [0x90] = {.r = "\\u0090", .l = 6}, [0x91] = {.r = "\\u0091", .l = 6},
      [0x92] = {.r = "\\u0092", .l = 6}, [0x93] = {.r = "\\u0093", .l = 6},
      [0x94] = {.r = "\\u0094", .l = 6}, [0x95] = {.r = "\\u0095", .l = 6},
      [0x96] = {.r = "\\u0096", .l = 6}, [0x97] = {.r = "\\u0097", .l = 6},
      [0x98] = {.r = "\\u0098", .l = 6}, [0x99] = {.r = "\\u0099", .l = 6},
      [0x9a] = {.r = "\\u009a", .l = 6}, [0x9b] = {.r = "\\u009b", .l = 6},
      [0x9c] = {.r = "\\u009c", .l = 6}, [0x9d] = {.r = "\\u009d", .l = 6},
      [0x9e] = {.r = "\\u009e", .l = 6}, [0x9f] = {.r = "\\u009f", .l = 6},
      [0xa0] = {.r = "\\u00a0", .l = 6}, [0xa1] = {.r = "\\u00a1", .l = 6},
      [0xa2] = {.r = "\\u00a2", .l = 6}, [0xa3] = {.r = "\\u00a3", .l = 6},
      [0xa4] = {.r = "\\u00a4", .l = 6}, [0xa5] = {.r = "\\u00a5", .l = 6},
      [0xa6] = {.r = "\\u00a6", .l = 6}, [0xa7] = {.r = "\\u00a7", .l = 6},
      [0xa8] = {.r = "\\u00a8", .l = 6}, [0xa9] = {.r = "\\u00a9", .l = 6},
      [0xaa] = {.r = "\\u00aa", .l = 6}, [0xab] = {.r = "\\u00ab", .l = 6},
      [0xac] = {.r = "\\u00ac", .l = 6}, [0xad] = {.r = "\\u00ad", .l = 6},
      [0xae] = {.r = "\\u00ae", .l = 6}, [0xaf] = {.r = "\\u00af", .l = 6},
      [0xb0] = {.r = "\\u00b0", .l = 6}, [0xb1] = {.r = "\\u00b1", .l = 6},
      [0xb2] = {.r = "\\u00b2", .l = 6}, [0xb3] = {.r = "\\u00b3", .l = 6},
      [0xb4] = {.r = "\\u00b4", .l = 6}, [0xb5] = {.r = "\\u00b5", .l = 6},
      [0xb6] = {.r = "\\u00b6", .l = 6}, [0xb7] = {.r = "\\u00b7", .l = 6},
      [0xb8] = {.r = "\\u00b8", .l = 6}, [0xb9] = {.r = "\\u00b9", .l = 6},
      [0xba] = {.r = "\\u00ba", .l = 6}, [0xbb] = {.r = "\\u00bb", .l = 6},
      [0xbc] = {.r = "\\u00bc", .l = 6}, [0xbd] = {.r = "\\u00bd", .l = 6},
      [0xbe] = {.r = "\\u00be", .l = 6}, [0xbf] = {.r = "\\u00bf", .l = 6},
      [0xc0] = {.r = "\\u00c0", .l = 6}, [0xc1] = {.r = "\\u00c1", .l = 6},
      [0xc2] = {.r = "\\u00c2", .l = 6}, [0xc3] = {.r = "\\u00c3", .l = 6},
      [0xc4] = {.r = "\\u00c4", .l = 6}, [0xc5] = {.r = "\\u00c5", .l = 6},
      [0xc6] = {.r = "\\u00c6", .l = 6}, [0xc7] = {.r = "\\u00c7", .l = 6},
      [0xc8] = {.r = "\\u00c8", .l = 6}, [0xc9] = {.r = "\\u00c9", .l = 6},
      [0xca] = {.r = "\\u00ca", .l = 6}, [0xcb] = {.r = "\\u00cb", .l = 6},
      [0xcc] = {.r = "\\u00cc", .l = 6}, [0xcd] = {.r = "\\u00cd", .l = 6},
      [0xce] = {.r = "\\u00ce", .l = 6}, [0xcf] = {.r = "\\u00cf", .l = 6},
      [0xd0] = {.r = "\\u00d0", .l = 6}, [0xd1] = {.r = "\\u00d1", .l = 6},
      [0xd2] = {.r = "\\u00d2", .l = 6}, [0xd3] = {.r = "\\u00d3", .l = 6},
      [0xd4] = {.r = "\\u00d4", .l = 6}, [0xd5] = {.r = "\\u00d5", .l = 6},
      [0xd6] = {.r = "\\u00d6", .l = 6}, [0xd7] = {.r = "\\u00d7", .l = 6},
      [0xd8] = {.r = "\\u00d8", .l = 6}, [0xd9] = {.r = "\\u00d9", .l = 6},
      [0xda] = {.r = "\\u00da", .l = 6}, [0xdb] = {.r = "\\u00db", .l = 6},
      [0xdc] = {.r = "\\u00dc", .l = 6}, [0xdd] = {.r = "\\u00dd", .l = 6},
      [0xde] = {.r = "\\u00de", .l = 6}, [0xdf] = {.r = "\\u00df", .l = 6},
      [0xe0] = {.r = "\\u00e0", .l = 6}, [0xe1] = {.r = "\\u00e1", .l = 6},
      [0xe2] = {.r = "\\u00e2", .l = 6}, [0xe3] = {.r = "\\u00e3", .l = 6},
      [0xe4] = {.r = "\\u00e4", .l = 6}, [0xe5] = {.r = "\\u00e5", .l = 6},
      [0xe6] = {.r = "\\u00e6", .l = 6}, [0xe7] = {.r = "\\u00e7", .l = 6},
      [0xe8] = {.r = "\\u00e8", .l = 6}, [0xe9] = {.r = "\\u00e9", .l = 6},
      [0xea] = {.r = "\\u00ea", .l = 6}, [0xeb] = {.r = "\\u00eb", .l = 6},
      [0xec] = {.r = "\\u00ec", .l = 6}, [0xed] = {.r = "\\u00ed", .l = 6},
      [0xee] = {.r = "\\u00ee", .l = 6}, [0xef] = {.r = "\\u00ef", .l = 6},
      [0xf0] = {.r = "\\u00f0", .l = 6}, [0xf1] = {.r = "\\u00f1", .l = 6},
      [0xf2] = {.r = "\\u00f2", .l = 6}, [0xf3] = {.r = "\\u00f3", .l = 6},
      [0xf4] = {.r = "\\u00f4", .l = 6}, [0xf5] = {.r = "\\u00f5", .l = 6},
      [0xf6] = {.r = "\\u00f6", .l = 6}, [0xf7] = {.r = "\\u00f7", .l = 6},
      [0xf8] = {.r = "\\u00f8", .l = 6}, [0xf9] = {.r = "\\u00f9", .l = 6},
      [0xfa] = {.r = "\\u00fa", .l = 6}, [0xfb] = {.r = "\\u00fb", .l = 6},
      [0xfc] = {.r = "\\u00fc", .l = 6}, [0xfd] = {.r = "\\u00fd", .l = 6},
      [0xfe] = {.r = "\\u00fe", .l = 6}, [0xff] = {.r = "\\u00ff", .l = 6}};
  const __escape_t* entry = &escapes[c];
  return ( bs_append( sink, ( const uint8_t* )entry->r, entry->l ) );
}

static inline int bs_append_escape(
    bytestring_sink_t* bs, const uint8_t* p, size_t n ) {
  int64_t ok = 0;
  for( size_t i = 0; i < n; i++ ) { ok += bs_sink_escape( bs, p[i] ); }
  return ( ok < 0 ? -1 : 0 );
}

static inline int bs_cat_escape(
    bytestring_sink_t* bs, const char* p, size_t n ) {
  return ( bs_append_escape( bs, ( const uint8_t* )p, n ) );
}

#endif