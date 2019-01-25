/*
   balboa
   Copyright (c) 2018, DCSO GmbH
*/

#include "obs_rocksdb.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "rocksdb/c.h"

extern void cgoLogInfo(const char*);
extern void cgoLogDebug(const char*);
extern void cgoObsDump(Observation*);

#define OBS_SET_STEP_SIZE 1000

static inline void _write_u32_le( unsigned char* p, uint32_t v ){
    p[0]=v>>0;
    p[1]=v>>8;
    p[2]=v>>16;
    p[3]=v>>24;
}

static inline uint32_t _read_u32_le( const unsigned char* p ){
    return(
        (((uint32_t)p[0])<<0)
       |(((uint32_t)p[1])<<8)
       |(((uint32_t)p[2])<<16)
       |(((uint32_t)p[3])<<24)
    );
}

struct Error {
    char *msg;
};

Error* error_new()
{
    Error *e = calloc(1, sizeof(Error));
    if (!e) {
        return NULL;
    }
    return e;
}

const char* error_get(Error *e)
{
    if (!e) {
        return NULL;
    }
    return e->msg;
}

static void error_set(Error *e, const char *msg)
{
    if (!e) {
        return;
    }
    if (!msg) {
        return;
    }
    if (e->msg) {
        free(e->msg);
    }
    e->msg = strdup(msg);
}

void error_unset(Error *e)
{
    if (!e) {
        return;
    }
    if (e->msg) {
        free(e->msg);
    }
    e->msg = NULL;
}

bool error_is_set(Error *e)
{
    if (!e) {
        return NULL;
    }
    return (e->msg != NULL);
}

void error_delete(Error *e)
{
    if (!e) {
        return;
    }
    if (e->msg) {
        free(e->msg);
    }
    free(e);
}


struct ObsSet {
    unsigned long size, used;
    Observation **os;
};

static ObsSet* obs_set_create(unsigned long size)
{
    ObsSet *os = calloc((size_t) 1, sizeof(ObsSet));
    if (!os) {
        return NULL;
    }
    os->size = size;
    os->used = 0;
    os->os = calloc((size_t) size, sizeof(Observation*));
    if (os->os == NULL) {
        free(os);
        return NULL;
    }
    return os;
}

unsigned long obs_set_size(ObsSet *os)
{
    if (!os) {
        return 0;
    }
    return os->used;
}

static void obs_set_add(ObsSet *os, Observation *o)
{
    if (!os || !os->os) {
        return;
    }
    if (os->used == os->size) {
        os->size += OBS_SET_STEP_SIZE;
        os->os = realloc(os->os, (size_t) (os->size * sizeof(Observation*)));
    }
    os->os[os->used++] = o;
}

const Observation* obs_set_get(ObsSet *os, unsigned long i)
{
    if (!os) {
        return NULL;
    }
    if (i >= os->used) {
        return NULL;
    }
    return os->os[i];
}

void obs_set_delete(ObsSet *os)
{
    if (!os) {
        return;
    }
    if (os->os != NULL) {
        unsigned long i = 0;
        for (i = 0; i < os->used; i++) {
            if (os->os[i]->key)
                free(os->os[i]->key);
            if (os->os[i]->inv_key)
                free(os->os[i]->inv_key);
            free(os->os[i]);
        }
        free(os->os);
    }
    free(os);
}

struct ObsDB {
    rocksdb_t *db;
    rocksdb_options_t *options;
    rocksdb_writeoptions_t *writeoptions;
    rocksdb_readoptions_t *readoptions;
    rocksdb_mergeoperator_t *mergeop;
};

#define VALUE_LENGTH (sizeof(uint32_t) + (2 * sizeof(time_t)))

#define obsdb_max(a,b) \
   ({ __typeof__ (a) _a = (a); \
       __typeof__ (b) _b = (b); \
     _a > _b ? _a : _b; })

#define obsdb_min(a,b) \
   ({ __typeof__ (a) _a = (a); \
       __typeof__ (b) _b = (b); \
     _a < _b ? _a : _b; })

static inline int obs2buf(Observation *o, char *buf, size_t buflen) {
    size_t minlen = sizeof(uint32_t)*3;
    if (buflen < minlen) {
        cgoLogInfo("serialize observation: buffer too short");
        return -1;
    }

    unsigned char* p = (unsigned char*)buf;
    _write_u32_le(p+0,o->count);
    _write_u32_le(p+4,o->last_seen);
    _write_u32_le(p+8,o->first_seen);

    return 0;
}

static inline int buf2obs(Observation *o, const char *buf, size_t buflen) {
    size_t minlen = sizeof(uint32_t)*3;
    if (buflen < minlen) {
        cgoLogInfo("deserialize observation: buffer too short");
        return -1;
    };

    const unsigned char* p = (const unsigned char*)buf;
    o->count = _read_u32_le(p+0);
    o->last_seen = _read_u32_le(p+4);
    o->first_seen = _read_u32_le(p+8);

    return 0;
}

static char* obsdb_mergeop_full_merge(void *state, const char* key,
                               size_t key_length, const char* existing_value,
                               size_t existing_value_length,
                               const char* const* operands_list,
                               const size_t* operands_list_length,
                               int num_operands, unsigned char* success,
                               size_t* new_value_length)
{
    Observation obs = {NULL, NULL, 0, 0, 0};

    if (key_length < 1) {
        cgoLogInfo("full merge: key too short");
        *success = (unsigned char) 0;
        return NULL;
    }
    if (key[0] == 'i') {
        /* this is an inverted index key with no meaningful value */
        char *res = malloc(sizeof(char)*1);
        if (res==NULL) {
            return NULL;
        }
        *res = '\0';
        *new_value_length = 1;
        *success = 1;
        return res;
    } else if (key[0] == 'o') {
        /* this is an observation value */
        int i;
        size_t buflength = sizeof(uint32_t)*3;
        char *buf = malloc(buflength);
        if (buf==NULL) { return NULL; }
        if (existing_value) {
            buf2obs(&obs, existing_value, existing_value_length);
        }
        for (i = 0; i < num_operands; i++) {
            Observation nobs = {NULL, NULL, 0, 0, 0};
            buf2obs(&nobs, operands_list[i], operands_list_length[i]);
            if (i == 0) {
                if (existing_value == NULL) {
                    obs.count = nobs.count;
                    obs.last_seen = nobs.last_seen;
                    obs.first_seen = nobs.first_seen;
                } else {
                    obs.count += nobs.count;
                    obs.last_seen = obsdb_max(obs.last_seen, nobs.last_seen);
                    obs.first_seen = obsdb_min(obs.first_seen, nobs.first_seen);
                }
            } else {
                obs.count += nobs.count;
                obs.last_seen = obsdb_max(obs.last_seen, nobs.last_seen);
                obs.first_seen = obsdb_min(obs.first_seen, nobs.first_seen);
            }
        }
        obs2buf(&obs, buf, buflength);
        *new_value_length = buflength;
        *success = (unsigned char) 1;
        return buf;
    } else {
        /* weird key format! */
        cgoLogInfo("full merge: weird key format");
        *success = (unsigned char) 0;
        return NULL;
    }
}

static char* obsdb_mergeop_partial_merge(void *state, const char* key,
                                  size_t key_length,
                                  const char* const* operands_list,
                                  const size_t* operands_list_length,
                                  int num_operands, unsigned char* success,
                                  size_t* new_value_length)
{
    Observation obs = {NULL, NULL, 0, 0, 0};

    if (key_length < 1) {
        cgoLogInfo("partial merge: key too short");
        *success = (unsigned char) 0;
        return NULL;
    }
    if (key[0] == 'i') {
        /* this is an inverted index key with no meaningful value */
        char *res = malloc(sizeof(char)*1);
        if (res == NULL) { return NULL; }
        *res = '\0';
        *new_value_length = 1;
        *success = 1;
        return res;
    } else if (key[0] == 'o') {
        /* this is an observation value */
        int i;
        size_t buflength=sizeof(uint32_t)*3;
        char *buf = malloc(buflength);
        if (buf==NULL) { return NULL; }
        for (i = 0; i < num_operands; i++) {
            Observation nobs = {NULL, NULL, 0, 0, 0};
            buf2obs(&nobs, operands_list[i], operands_list_length[i]);
            if (i == 0) {
                obs.count = nobs.count;
                obs.last_seen = nobs.last_seen;
                obs.first_seen = nobs.first_seen;
            } else {
                obs.count += nobs.count;
                obs.last_seen = obsdb_max(obs.last_seen, nobs.last_seen);
                obs.first_seen = obsdb_min(obs.first_seen, nobs.first_seen);
            }
        }
        obs2buf(&obs, buf, buflength);
        *new_value_length = buflength;
        *success = (unsigned char) 1;
        return buf;
    } else {
        /* weird key format! */
        cgoLogInfo("partial merge: weird key format");
        *success = (unsigned char) 0;
        return NULL;
    }
}

static void obsdb_mergeop_destructor(void *state)
{
    return;
}

static const char* obsdb_mergeop_name(void *state)
{
    return "observation mergeop";
}

static ObsDB* _obsdb_open(const char *path, size_t membudget, Error *e, bool readonly)
{
    char *err = NULL;
    int level_compression[5] = {
        rocksdb_lz4_compression,
        rocksdb_lz4_compression,
        rocksdb_lz4_compression,
        rocksdb_lz4_compression,
        rocksdb_lz4_compression
    };
    ObsDB *db = calloc(1, sizeof(ObsDB));
    if (db == NULL) {
        if (e) {
            error_set(e, "could not allocate memory");
        }
        return NULL;
    }

    db->mergeop = rocksdb_mergeoperator_create(NULL,
                                               obsdb_mergeop_destructor,
                                               obsdb_mergeop_full_merge,
                                               obsdb_mergeop_partial_merge,
                                               NULL,
                                               obsdb_mergeop_name);

    db->options = rocksdb_options_create();
    rocksdb_options_increase_parallelism(db->options, 8);
    if (!readonly) {
        rocksdb_options_optimize_level_style_compaction(db->options, membudget);
    }
    rocksdb_options_set_create_if_missing(db->options, 1);
    rocksdb_options_set_max_log_file_size(db->options, 10*1024*1024);
    rocksdb_options_set_keep_log_file_num(db->options, 2);
    rocksdb_options_set_max_open_files(db->options, 300);
    rocksdb_options_set_merge_operator(db->options, db->mergeop);
    rocksdb_options_set_compression_per_level(db->options, level_compression, 5);

    if (!readonly) {
        db->db = rocksdb_open(db->options, path, &err);
    } else {
        db->db = rocksdb_open_for_read_only(db->options, path, 0, &err);
    }
    if (err) {
        if (e) {
            error_set(e, err);
        }
        free(err);
        return NULL;
    }

    db->writeoptions = rocksdb_writeoptions_create();
    db->readoptions = rocksdb_readoptions_create();

    return db;
}

ObsDB* obsdb_open(const char *path, size_t membudget, Error *e) {
    return _obsdb_open(path, membudget, e, false);
}

ObsDB* obsdb_open_readonly(const char *path, Error *e) {
    return _obsdb_open(path, 0, e, true);
}

int obsdb_put(ObsDB *db, Observation *obs, Error *e)
{
    char *err = NULL;
    char buf[sizeof(uint32_t)*3];
    size_t buflength = sizeof(buf);

    if (!db) {
        return -1;
    }

    (void) obs2buf(obs, buf, buflength);

    rocksdb_merge(db->db, db->writeoptions, obs->key, strlen(obs->key),
                buf, buflength, &err);
    if (err) {
        if (e) {
            error_set(e, err);
        }
        free(err);
        return -1;

    }

    rocksdb_merge(db->db, db->writeoptions, obs->inv_key, strlen(obs->key),
                "", 0, &err);
    if (err) {
        if (e) {
            error_set(e, err);
        }
        free(err);
        return -1;
    }

    return 0;
}

int obsdb_dump(ObsDB *db, Error *e)
{
    rocksdb_iterator_t *it;
    if (!db) {
        return -1;
    }

    it = rocksdb_create_iterator(db->db, db->readoptions);
    for (rocksdb_iter_seek(it, "o", 1);
         rocksdb_iter_valid(it) != (unsigned char) 0;
         rocksdb_iter_next(it)) {
        size_t size = 0;
        int ret = 0;
        Observation *o = NULL;
        const char *rkey = NULL, *val = NULL;

        rkey = rocksdb_iter_key(it, &size);
        o = calloc(1, sizeof(Observation));
        if (!o) {
            return -1;
        }
        o->key = calloc(size + 1, sizeof(char));
        if (!o->key) {
            free(o);
            return -1;
        }
        strncpy(o->key, rkey, size);
        o->key[size] = '\0';
        val = rocksdb_iter_value(it, &size);
        ret = buf2obs(o, val, size);
        if (ret != 0) {
            cgoLogInfo("error");
        }
        cgoObsDump(o);
        free(o->key);
        free(o);
    }
    rocksdb_iter_destroy(it);

    return 0;
}

ObsSet* obsdb_search(ObsDB *db, const char *qrdata, const char *qrrname,
                         const char *qrrtype, const char *qsensorID)
{
    rocksdb_iterator_t *it;
    ObsSet *os;
    if (!db) {
        return NULL;
    }

    os = obs_set_create(OBS_SET_STEP_SIZE);

    if (qrrname != NULL) {
        char *prefix = NULL;
        size_t prefixlen = 0;
        if (qsensorID != NULL) {
            prefixlen = strlen(qsensorID) + strlen(qrrname) + 4;
            prefix = calloc(prefixlen+1, sizeof(char));
            if (!prefix) {
                obs_set_delete(os);
                return NULL;
            }
            (void) snprintf(prefix, prefixlen, "o\x1f%s\x1f%s\x1f", qrrname, qsensorID);
        } else {
            prefixlen = strlen(qrrname) + 3;
            prefix = calloc(prefixlen+1, sizeof(char));
            if (!prefix) {
                obs_set_delete(os);
                return NULL;
            }
            (void) snprintf(prefix, prefixlen, "o\x1f%s\x1f", qrrname);
        }
        cgoLogDebug(prefix);

        it = rocksdb_create_iterator(db->db, db->readoptions);
        rocksdb_iter_seek(it, prefix, prefixlen);
        for (; rocksdb_iter_valid(it) != (unsigned char) 0; rocksdb_iter_next(it)) {
            size_t size = 0;
            int ret = 0;
            Observation *o = NULL;
            const char *rkey = rocksdb_iter_key(it, &size), *val = NULL;
            char *rrname = NULL, *sensorID = NULL, *rrtype = NULL, *rdata = NULL, *saveptr;
            char *tokkey = calloc(size + 1,  sizeof(char));
            if (!tokkey) {
                obs_set_delete(os);
                rocksdb_iter_destroy(it);
                free(prefix);
                return NULL;
            }

            strncpy(tokkey, rkey, size);
            tokkey[size] = '\0';
            rrname = strtok_r(tokkey+2, "\x1f", &saveptr);
            sensorID = strtok_r(NULL, "\x1f", &saveptr);
            rrtype = strtok_r(NULL, "\x1f", &saveptr);
            rdata = strtok_r(NULL, "\x1f", &saveptr);
            if (rrname == NULL || (strcmp(rrname, qrrname) != 0)) {
                free(tokkey);
                tokkey = NULL;
                break;
            }
            if (sensorID == NULL || (qsensorID != NULL && strcmp(qsensorID, sensorID) != 0)) {
                free(tokkey);
                tokkey = NULL;
                continue;
            }
            if (rdata == NULL || (qrdata != NULL && strcmp(qrdata, rdata) != 0)) {
                free(tokkey);
                tokkey = NULL;
                continue;
            }
            if (rrtype == NULL || (qrrtype != NULL && strcmp(qrrtype, rrtype) != 0)) {
                free(tokkey);
                tokkey = NULL;
                continue;
            }

            o = calloc(1, sizeof(Observation));
            if (!o) {
                free(tokkey);
                obs_set_delete(os);
                rocksdb_iter_destroy(it);
                free(prefix);
                return NULL;
            }
            o->key = calloc(size + 1, sizeof(char));
            if (!o->key) {
                free(o);
                free(tokkey);
                obs_set_delete(os);
                rocksdb_iter_destroy(it);
                free(prefix);
                return NULL;
            }
            strncpy(o->key, rkey, size);
            o->key[size] = '\0';
            val = rocksdb_iter_value(it, &size);
            ret = buf2obs(o, val, size);
            if (ret == 0) {
                obs_set_add(os, o);
            }
            free(tokkey);
            tokkey = NULL;
        }
        rocksdb_iter_destroy(it);
        free(prefix);
    } else {
        char *prefix = NULL;
        size_t prefixlen = 0;
        if (qsensorID != NULL) {
            prefixlen = strlen(qsensorID) + strlen(qrdata) + 4;
            prefix = calloc(prefixlen+1, sizeof(char));
            if (!prefix) {
                obs_set_delete(os);
                return NULL;
            }
            (void) snprintf(prefix, prefixlen, "i\x1f%s\x1f%s\x1f", qrdata, qsensorID);
        } else {
            prefixlen = strlen(qrdata) + 3;
            prefix = calloc(prefixlen+1, sizeof(char));
            if (!prefix) {
                obs_set_delete(os);
                return NULL;
            }
            (void) snprintf(prefix, prefixlen, "i\x1f%s\x1f", qrdata);
        }
        cgoLogDebug(prefix);

        it = rocksdb_create_iterator(db->db, db->readoptions);
        rocksdb_iter_seek(it, prefix, strlen(prefix));
        for (; rocksdb_iter_valid(it) != (unsigned char) 0; rocksdb_iter_next(it)) {
            size_t size = 0, fullkeylen;
            int ret;
            const char *rkey = rocksdb_iter_key(it, &size);
            char *val = NULL;
            char *rrname = NULL, *sensorID = NULL, *rrtype = NULL, *rdata = NULL, *saveptr;
            char fullkey[4096];
            char *err = NULL;
            Observation *o = NULL;
            char *tokkey = calloc(size + 1,  sizeof(char));
            if (!tokkey) {
                obs_set_delete(os);
                rocksdb_iter_destroy(it);
                free(prefix);
                return NULL;
            }

            strncpy(tokkey, rkey, size);
            tokkey[size] = '\0';
            rdata = strtok_r(tokkey+2, "\x1f", &saveptr);
            sensorID = strtok_r(NULL, "\x1f", &saveptr);
            rrname = strtok_r(NULL, "\x1f", &saveptr);
            rrtype = strtok_r(NULL, "\x1f", &saveptr);
            if (strcmp(rdata, qrdata) != 0) {
                free(tokkey);
                tokkey = NULL;
                break;
            }
            cgoLogDebug(rdata);
            if (sensorID == NULL || (qsensorID != NULL && strcmp(qsensorID, sensorID) != 0)) {
                free(tokkey);
                tokkey = NULL;
                continue;
            }
            if (rdata == NULL || (qrdata != NULL && strcmp(qrdata, rdata) != 0)) {
                free(tokkey);
                tokkey = NULL;
                continue;
            }
            if (rrtype == NULL || (qrrtype != NULL && strcmp(qrrtype, rrtype) != 0)) {
                free(tokkey);
                tokkey = NULL;
                continue;
            }

            bzero(fullkey,4096);
            (void) snprintf(fullkey, 4095, "o\x1f%s\x1f%s\x1f%s\x1f%s", rrname, sensorID, rrtype, rdata);

            cgoLogDebug(fullkey);

            fullkeylen = strlen(fullkey);
            val = rocksdb_get(db->db, db->readoptions, fullkey, fullkeylen, &size, &err);
            if (err != NULL) {
                cgoLogDebug("observation not found");
                free(tokkey);
                tokkey = NULL;
                continue;
            }

            o = calloc(1, sizeof(Observation));
            if (!o) {
                free(tokkey);
                obs_set_delete(os);
                rocksdb_iter_destroy(it);
                free(prefix);
                free(val);
                return NULL;
            }
            o->key = calloc(fullkeylen + 1, sizeof(char));
            if (o->key==NULL) {
                free(o);
                free(tokkey);
                obs_set_delete(os);
                rocksdb_iter_destroy(it);
                free(prefix);
                free(val);
                return NULL;
            }
            strncpy(o->key, fullkey, fullkeylen);
            o->key[fullkeylen] = '\0';
            ret = buf2obs(o, val, size);
            if (ret == 0) {
                obs_set_add(os, o);
            }
            free(tokkey);
            free(val);
            tokkey = NULL;
        }
        rocksdb_iter_destroy(it);
        free(prefix);
    }

    return os;
}

unsigned long obsdb_num_keys(ObsDB *db)
{
    const char *val;
    if (!db) {
        return 0;
    }
    val = rocksdb_property_value(db->db, "rocksdb.estimate-num-keys");
    if (!val) {
        return 0;
    } else {
        unsigned long v;
        int ret;
        ret = sscanf(val, "%lu", &v);
        if (ret != 1) {
            return 0;
        } else {
            return v;
        }
    }
}

void obsdb_close(ObsDB *db)
{
    if (!db) {
        return;
    }
    rocksdb_mergeoperator_destroy(db->mergeop);
    rocksdb_writeoptions_destroy(db->writeoptions);
    rocksdb_readoptions_destroy(db->readoptions);
    // rocksdb_options_destroy(db->options);
    rocksdb_close(db->db);
}
