
#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <syslog.h>
#include <unistd.h>

#include <daemon.h>

int daemonize() {
  pid_t pid1 = fork();
  if( pid1 < 0 ) {
    return ( -1 );
  } else if( pid1 != 0 ) {
    _exit( 0 );
  }
  pid_t sid = setsid();
  if( sid < 0 ) { return ( -1 ); }
  pid_t pid2 = fork();
  if( pid2 < 0 ) {
    return ( -1 );
  } else if( pid2 != 0 ) {
    _exit( 0 );
  }

  for( int n = 0; n < 1024; n++ ) { close( n ); }

  int fd = open( "/dev/null", O_RDWR );
  if( fd < 0 ) { return ( 0 ); }
  dup2( fd, 0 );
  dup2( fd, 1 );
  dup2( fd, 2 );

  return ( 0 );
}