// balboa
// Copyright (c) 2018, 2019 DCSO GmbH

#ifndef __ALLOC_H
#define __ALLOC_H

#include <inttypes.h>
#include <stdlib.h>
#include <trace.h>

static inline void* blb_realloc_impl(
    const char* name, void* p, size_t new_size ) {
  void* pp = realloc( p, new_size );
  X( log_debug( "(%s) realloc `%p` `%p` `%zu`", name, p, pp, new_size ) );
  return ( pp );
}

#define blb_new( ty )                                      \
  ( {                                                      \
    size_t p_sz = sizeof( ty );                            \
    void* p = malloc( p_sz );                              \
    X( log_debug( "new `%s` `%zu` `%p`", #ty, p_sz, p ) ); \
    p;                                                     \
  } )
#define blb_malloc( sz )                           \
  ( {                                              \
    size_t p_sz = ( sz );                          \
    void* p = malloc( p_sz );                      \
    X( log_debug( "alloc `%zu` `%p`", p_sz, p ) ); \
    p;                                             \
  } )
#define blb_realloc( p, sz )                                  \
  ( {                                                         \
    size_t p_sz = ( sz );                                     \
    void* pp = realloc( ( p ), p_sz );                        \
    X( log_debug( "realloc `%p` `%zu` `%p`", pp, p_sz, p ) ); \
    pp;                                                       \
  } )
#define blb_free( p )                     \
  do {                                    \
    X( log_debug( "free `%p`", ( p ) ) ); \
    free( p );                            \
  } while( 0 )

#endif