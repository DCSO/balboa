// balboa
// Copyright (c) 2019 DCSO GmbH

#ifndef __TRACE_H
#define __TRACE_H

#include <pthread.h>
#include <stdarg.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>

#define V(body)                        \
  do {                                 \
    if(theTrace_get_verbosity() > 0) { \
      theTrace_lock();                 \
      body;                            \
      theTrace_release();              \
    }                                  \
  } while(0)
#define T(body)                        \
  do {                                 \
    if(theTrace_get_verbosity() > 1) { \
      theTrace_lock();                 \
      body;                            \
      theTrace_release();              \
    }                                  \
  } while(0)
#define X(body)                        \
  do {                                 \
    if(theTrace_get_verbosity() > 2) { \
      theTrace_lock();                 \
      body;                            \
      theTrace_release();              \
    }                                  \
  } while(0)
#define L(body)         \
  do {                  \
    theTrace_lock();    \
    body;               \
    theTrace_release(); \
  } while(0)

#define log_when(body) if(body)
#define verbosity(lvl) (theTrace_get_verbosity() >= 0)
#define log_enter() theTrace_lock()
#define log_leave() theTrace_release()

// 0 Emergency: system is unusable
// 1 Alert: action must be taken immediately
// 2 Critical: critical conditions
// 3 Error: error conditions
// 4 Warning: warning conditions
// 5 Notice: normal but significant condition
// 6 Informational: informational messages
// 7 Debug: debug-level messages
#define log_emergency(fmt, ...)                                        \
  do {                                                                 \
    theTrace_output(0, "(%s) " fmt "\n", __FUNCTION__, ##__VA_ARGS__); \
    exit(1);                                                           \
  } while(0)
#define log_alert(fmt, ...) \
  theTrace_output(1, "(%s) " fmt "\n", __FUNCTION__, ##__VA_ARGS__)
#define log_crit(fmt, ...) \
  theTrace_output(2, "(%s) " fmt "\n", __FUNCTION__, ##__VA_ARGS__)
#define log_error(fmt, ...) \
  theTrace_output(3, "(%s) " fmt "\n", __FUNCTION__, ##__VA_ARGS__)
#define log_warn(fmt, ...) \
  theTrace_output(4, "(%s) " fmt "\n", __FUNCTION__, ##__VA_ARGS__)
#define log_notice(fmt, ...) \
  theTrace_output(5, "(%s) " fmt "\n", __FUNCTION__, ##__VA_ARGS__)
#define log_info(fmt, ...) \
  theTrace_output(6, "(%s) " fmt "\n", __FUNCTION__, ##__VA_ARGS__)
#define log_debug(fmt, ...) \
  theTrace_output(7, "(%s) " fmt "\n", __FUNCTION__, ##__VA_ARGS__)
#define log_inject(fmt, ...) theTrace_inject(fmt, ##__VA_ARGS__)

#define WHEN_V if(theTrace_get_verbosity() > 0)
#define WHEN_T if(theTrace_get_verbosity() > 1)
#define WHEN_X if(theTrace_get_verbosity() > 2)

#define ASSERT(p)                          \
  do {                                     \
    if(!(p)) {                             \
      log_emergency(                       \
          "assert failed `%s` (%s:%s:%d)", \
          #p,                              \
          __FILE__,                        \
          __FUNCTION__,                    \
          __LINE__);                       \
    }                                      \
  } while(0)

typedef struct trace_config_t trace_config_t;
struct trace_config_t {
  int verbosity;
  FILE* stream;
  const char* host;
  const char* app;
  pid_t procid;
  bool rfc5424;
};

typedef struct trace_t trace_t;
struct trace_t {
  atomic_int verbosity;
  pthread_mutex_t _lock;
  trace_config_t config;
  void (*init)(trace_t* trace, const trace_config_t* config);
  void (*lock)(trace_t* trace);
  void (*release)(trace_t* trace);
  void (*output)(trace_t* trace, int priority, const char* fmt, va_list ap);
  void (*inject)(trace_t* trace, const char* fmt, va_list ap);
  void (*flush)(trace_t* trace);
};

void theTrace_set(trace_t* trace);
trace_t* theTrace_get(void);
void theTrace_stream_use(const trace_config_t* config);

__attribute__((format(printf, 2, 3))) static inline void theTrace_output(
    int priority, const char* fmt, ...) {
  va_list ap;
  va_start(ap, fmt);
  theTrace_get()->output(theTrace_get(), priority, fmt, ap);
  va_end(ap);
}

__attribute__((format(printf, 1, 2))) static inline void theTrace_inject(
    const char* fmt, ...) {
  va_list ap;
  va_start(ap, fmt);
  theTrace_get()->inject(theTrace_get(), fmt, ap);
  va_end(ap);
}

static inline void theTrace_lock(void) {
  theTrace_get()->lock(theTrace_get());
}

static inline void theTrace_release(void) {
  theTrace_get()->release(theTrace_get());
}

static inline int theTrace_get_verbosity(void) {
  return (atomic_load(&theTrace_get()->verbosity));
}

static inline void theTrace_set_verbosity(int verbosity) {
  atomic_store(&theTrace_get()->verbosity, verbosity);
}

static inline void theTrace_init(const trace_config_t* config) {
  theTrace_get()->init(theTrace_get(), config);
}

static inline void theTrace_flush(void) {
  theTrace_get()->flush(theTrace_get());
}

#endif
