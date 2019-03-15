// balboa
// Copyright (c) 2018, 2019, DCSO GmbH

#include <assert.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include <rocksdb/c.h>

#include <mpack.h>
#include <tpl.h>

static int verbose=0;

#define V(body) if( verbose>0 ) { do{ body; }while( 0 );}

#define OBS_SET_STEP_SIZE 1000

typedef struct{
  char *key, *inv_key;
  uint32_t count,last_seen,first_seen;
}Observation;

typedef struct ObsDB ObsDB;
struct ObsDB{
    rocksdb_t* db;
    rocksdb_options_t* opt;
    rocksdb_readoptions_t* rd_opt;
    rocksdb_checkpoint_t* cp;
    rocksdb_mergeoperator_t* merge_ops;
    size_t scrtch_sz;
    uint8_t* scrtch;
};

enum{
    OBS_RRNAME_IDX=0
   ,OBS_RRTYPE_IDX=1
   ,OBS_RDATA_IDX=2
   ,OBS_SENSOR_IDX=3
   ,OBS_COUNT_IDX=4
   ,OBS_FIRST_SEEN_IDX=5
   ,OBS_LAST_SEEN_IDX=6
   ,OBS_FIELDS=7
};

#define obsdb_max(a,b) \
   ({ __typeof__ (a) _a = (a); \
       __typeof__ (b) _b = (b); \
     _a > _b ? _a : _b; })

#define obsdb_min(a,b) \
   ({ __typeof__ (a) _a = (a); \
       __typeof__ (b) _b = (b); \
     _a < _b ? _a : _b; })

static inline int obs2buf(Observation *o, char **buf, size_t *buflen) {
    uint32_t a,b,c;
    int ret = 0;
    tpl_node *tn = tpl_map("uuu", &a, &b, &c);
    a = o->count;
    b = o->last_seen;
    c = o->first_seen;
    ret = tpl_pack(tn, 0);
    if (ret == 0) {
        ret = tpl_dump(tn, TPL_MEM, buf, buflen);
    }
    tpl_free(tn);
    return ret;
}

static inline int buf2obs(Observation *o, const char *buf, size_t buflen) {
    uint32_t a,b,c;
    int ret = 0;
    tpl_node *tn = tpl_map("uuu", &a, &b, &c);
    ret = tpl_load(tn, TPL_MEM, buf, buflen);
    if (ret == 0) {
        (void) tpl_unpack(tn, 0);
        o->count = a;
        o->last_seen = b;
        o->first_seen = c;
    }
    tpl_free(tn);
    return ret;
}

static char* obsdb_mergeop_full_merge(void *state, const char* key,
                               size_t key_length, const char* existing_value,
                               size_t existing_value_length,
                               const char* const* operands_list,
                               const size_t* operands_list_length,
                               int num_operands, unsigned char* success,
                               size_t* new_value_length)
{
    (void)state;
    Observation obs = {NULL, NULL, 0, 0, 0};

    if (key_length < 1) {
        fprintf(stderr, "full merge: key too short\n");
        *success = (unsigned char) 0;
        return NULL;
    }
    if (key[0] == 'i') {
        /* this is an inverted index key with no meaningful value */
        char *res = malloc(1 * sizeof(char));
        *res = '\0';
        *new_value_length = 1;
        *success = 1;
        return res;
    } else if (key[0] == 'o') {
        /* this is an observation value */
        int i;
        size_t buflength;
        char *buf = NULL;
        if (existing_value) {
            buf2obs(&obs, existing_value, existing_value_length);
        }
        for (i = 0; i < num_operands; i++) {
            Observation nobs = {NULL, NULL, 0, 0, 0};
            buf2obs(&nobs, operands_list[i], operands_list_length[i]);
            if (i == 0) {
                if (!existing_value) {
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
        obs2buf(&obs, &buf, &buflength);
        *new_value_length = buflength;
        *success = (unsigned char) 1;
        return buf;
    } else {
        /* weird key format! */
        fprintf(stderr, "full merge: weird key format\n");
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
    (void)state;
    Observation obs = {NULL, NULL, 0, 0, 0};

    if (key_length < 1) {
        fprintf(stderr, "partial merge: key too short\n");
        *success = (unsigned char) 0;
        return NULL;
    }
    if (key[0] == 'i') {
        /* this is an inverted index key with no meaningful value */
        char *res = malloc(1 * sizeof(char));
        *res = '\0';
        *new_value_length = 1;
        *success = 1;
        return res;
    } else if (key[0] == 'o') {
        /* this is an observation value */
        int i;
        size_t buflength;
        char *buf = NULL;
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
        obs2buf(&obs, &buf, &buflength);
        *new_value_length = buflength;
        *success = (unsigned char) 1;
        return buf;
    } else {
        /* weird key format! */
        fprintf(stderr, "partial merge: weird key format\n");
        *success = (unsigned char) 0;
        return NULL;
    }
}

static void obsdb_mergeop_destructor(void *state){
    (void)state;
    return;
}

static const char* obsdb_mergeop_name(void *state){
    (void)state;
    return "observation mergeop";
}

static void dump_show( ObsDB* db,FILE* file,const char* key,size_t key_len,const char* val,size_t val_len ){
    enum TokIdx{
        RRNAME=0
       ,SENSORID=1
       ,RRTYPE=2
       ,RDATA=3
       ,FIELDS=4
    };

    struct Tok{
        const char* tok;
        int tok_len;
    };

    struct Tok toks[FIELDS]={NULL};

    enum TokIdx j=RRNAME;
    size_t last=1;
    for( size_t i=2;i<key_len;i++ ){
        if( key[i]=='\x1f' ){
            if( j<RDATA /*we fixup the RDATA and skip extra \x1f's*/ ){
                toks[j].tok=&key[last+1];
                toks[j].tok_len=i-last-1;
                last=i;
                j++;
            }
        }
    }
    toks[RDATA].tok=&key[last+1];
    toks[RDATA].tok_len=key_len-last-1;

    Observation obs={0};
    buf2obs(&obs,val,val_len);

    mpack_writer_t __wr={0},*wr=&__wr;
    mpack_writer_init(wr,(char*)db->scrtch,db->scrtch_sz);

    mpack_start_map(wr,OBS_FIELDS);
    mpack_write_uint(wr,OBS_RRNAME_IDX);
    mpack_write_bin(wr,toks[RRNAME].tok,toks[RRNAME].tok_len);
    mpack_write_uint(wr,OBS_RRTYPE_IDX);
    mpack_write_bin(wr,toks[RRTYPE].tok,toks[RRTYPE].tok_len);
    mpack_write_uint(wr,OBS_RDATA_IDX);
    mpack_write_bin(wr,toks[RDATA].tok,toks[RDATA].tok_len);
    mpack_write_uint(wr,OBS_SENSOR_IDX);
    mpack_write_bin(wr,toks[SENSORID].tok,toks[SENSORID].tok_len);
    mpack_write_uint(wr,OBS_COUNT_IDX);
    mpack_write_uint(wr,obs.count);
    mpack_write_uint(wr,OBS_FIRST_SEEN_IDX);
    mpack_write_uint(wr,obs.first_seen);
    mpack_write_uint(wr,OBS_LAST_SEEN_IDX);
    mpack_write_uint(wr,obs.last_seen);
    mpack_finish_map(wr);

    mpack_error_t err=mpack_writer_error(wr);
    if( err!=mpack_ok ){
        fprintf(stderr,"encoding msgpack data failed err=%d\n",err);
        mpack_writer_destroy(wr);
        return;
    }

    size_t used=mpack_writer_buffer_used(wr);
    V(fprintf(stderr,"encoded observation size=%zu\n",used));
    assert(used<db->scrtch_sz);

    size_t rc=fwrite(db->scrtch,used,1,file);
    if( rc!=1 ){
        fprintf(stderr,"fwrite failed error=%s\n",strerror(errno));
        mpack_writer_destroy(wr);
        return;
    }
    mpack_writer_destroy(wr);
}

static ssize_t dump( ObsDB *db,const char* db_path ){
    char* err_open=NULL;
    rocksdb_t* db_orig=rocksdb_open_for_read_only(db->opt,db_path,0,&err_open);
    db->db=NULL;
    if( db_orig==NULL ){
        fprintf(stderr,"db open error: %s\n",err_open==NULL?"<unknown>":err_open);
        if( err_open!=NULL ){ free(err_open); }
        return(-1);
    }
    db->db=db_orig;
    ssize_t cnt=0;
    rocksdb_iterator_t* it=rocksdb_create_iterator(db->db,db->rd_opt);
    if( it==NULL ) {
        fprintf(stderr,"unable to create iterator\n");
        return(1);
    }
    //rocksdb_iter_seek_to_first(it);
    rocksdb_iter_seek(it,"o",1);
    for( ;rocksdb_iter_valid(it)!=0;rocksdb_iter_next(it) ){
        size_t key_len=0;
        const char* key=rocksdb_iter_key(it,&key_len);
        size_t val_len=0;
        const char* val=rocksdb_iter_value(it,&val_len);
        if( val==NULL || key==NULL || key_len<1 || val_len<1 ){
            fprintf(stderr,"iterator failed fetching key and/or value\n");
            break;
        }
        if( key[0]=='i' ) { continue; }
        dump_show(db,stdout,key,key_len,val,val_len);
        cnt++;
    }
    char* err=NULL;
    rocksdb_iter_get_error(it,&err);
    if( err!=NULL ){
        fprintf(stderr,"iterator error: %s\n",err);
    }
    rocksdb_iter_destroy(it);
    return(cnt);
}

static int dump_checkpoint( ObsDB* db,const char* db_path,const char* cp_path ){
    char* err=NULL;
    rocksdb_t* db_orig=rocksdb_open(db->opt,db_path,&err);
    if( db_orig==NULL ){
        fprintf(stderr,"db open error: %s\n",err==NULL?"<unknown>":err);
        if( err!=NULL ){ free(err); }
        return(-1);
    }
    rocksdb_checkpoint_t* cp=rocksdb_checkpoint_object_create(db_orig,&err);
    if( cp==NULL ){
        fprintf(stderr,"checkpoint create object error: %s\n",err==NULL?"<unknown>":err);
        if( err!=NULL ){ free(err); }
        rocksdb_close(db_orig);
        return(-1);
    }
    err=NULL;
    rocksdb_checkpoint_create(cp,cp_path,0,&err);
    if( err!=NULL ){
        fprintf(stderr,"checkpoint create object error: %s\n",err==NULL?"<unknown>":err);
        if( err!=NULL ){ free(err); }
        rocksdb_checkpoint_object_destroy(cp);
        rocksdb_close(db_orig);
        return(-1);
    }
    rocksdb_checkpoint_object_destroy(cp);
    rocksdb_close(db_orig);
    return(0);
}

static int dump_obsdb_init( ObsDB* db ){
    db->scrtch_sz=1024*1024*10;
    db->scrtch=malloc(db->scrtch_sz);
    if( db->scrtch==NULL ) { return(-1); }
    return(0);
}

static void dump_obsdb_teardown( ObsDB* db ){
    free(db->scrtch);
}

static int dump_prepare_open( ObsDB* db ){
    int init_ok=dump_obsdb_init(db);
    if( init_ok!=0 ){ return(-1); }
    int level_compression[5]={
        rocksdb_lz4_compression
       ,rocksdb_lz4_compression
       ,rocksdb_lz4_compression
       ,rocksdb_lz4_compression
       ,rocksdb_lz4_compression
    };
    db->merge_ops=rocksdb_mergeoperator_create(
        NULL
       ,obsdb_mergeop_destructor
       ,obsdb_mergeop_full_merge
       ,obsdb_mergeop_partial_merge
       ,NULL
       ,obsdb_mergeop_name
    );
    if( db->merge_ops==NULL ){
        return(-1);
    }
    db->opt=rocksdb_options_create();
    if( db->opt==NULL ){
        rocksdb_mergeoperator_destroy(db->merge_ops);
        return(-1);
    }
    rocksdb_options_increase_parallelism(db->opt,8);
    rocksdb_options_set_create_if_missing(db->opt,0);
    rocksdb_options_set_max_log_file_size(db->opt,10*1024*1024);
    rocksdb_options_set_keep_log_file_num(db->opt,2);
    rocksdb_options_set_max_open_files(db->opt,300);
    rocksdb_options_set_merge_operator(db->opt,db->merge_ops);
    rocksdb_options_set_compression_per_level(db->opt,level_compression,5);
    db->rd_opt=rocksdb_readoptions_create();
    if( db->rd_opt==NULL ){
        rocksdb_options_destroy(db->opt);
        rocksdb_mergeoperator_destroy(db->merge_ops);
        return(-1);
    }
    return(0);
}

static void dump_close( ObsDB* db ){
    rocksdb_options_destroy(db->opt);
    rocksdb_mergeoperator_destroy(db->merge_ops);
    rocksdb_readoptions_destroy(db->rd_opt);
    rocksdb_close(db->db);
    dump_obsdb_teardown(db);
}

static int do_dump( const char* db_path ){
    ObsDB __db={0},*db=&__db;
    int ok=dump_prepare_open(db);
    if( ok!=0 ){
        fprintf(stderr,"opening database '%s' failed\n",db_path);
        return(-1);
    }
    ssize_t cnt=dump(db,db_path);
    if( cnt<0 ){
        fprintf(stderr,"dump failed\n");
        dump_close(db);
        return(-1);
    }
    fprintf(stderr,"dumped %zd entries\n",cnt);
    return(0);
}

static int do_checkpoint( const char* db_path,const char* cp_path ){
    ObsDB __db={0},*db=&__db;
    int ok=dump_prepare_open(db);
    if( ok!=0 ){
        fprintf(stderr,"preparing database options for '%s' failed\n",db_path);
        return(-1);
    }
    int cp_ok=dump_checkpoint(db,db_path,cp_path);
    if( cp_ok!=0 ){
        fprintf(stderr,"creating checkpoint failed\n");
        return(-1);
    }
    return(0);
}

int main( int argc,char** argv ){
    int res=-1;
    if( argc>2 ){
        if( strcmp(argv[1],"-v")==0 ) {
            verbose+=1;
            argc--;
            argv++;
        }
    }
    if( argc<3 ){
        goto usage;
    }else if( strcmp(argv[1],"checkpoint")==0 ){
        if( argc<4) { goto usage; }
        res=do_checkpoint(argv[2],argv[3]);
    }else if( strcmp(argv[1],"dump")==0 ){
        res=do_dump(argv[2]);
    }else{
        goto usage;
    }
    return(res);
usage:
    fprintf(stderr,"\n\
usage:\n\
\n\
    dump-rocksdb-v1 [-v] checkpoint <db-path> <checkpoint-path>\n\
        - create a checkpoint from balboa `db-path`. the checkpoint\n\
          will be stored at `checkpoint-path` using hard-links if possible\n\
\n\
    dump-rocksdb-v1 [-v] dump <checkpoint-path>\n\
        - dump all pDNS observations in `msgpack` format to stdout\n\
\n\
example:\n\
\n\
    dump-rocksdb-v1 checkpoint /mnt/balboa-rocksdb-live /mnt/balboa-rocksdb-checkpoint\n\
    dump-rocksdb-v1 dump /mnt/balboa-rocksdb-checkpoint | xz > /mnt/backup/balboa-dump.xz\n\
\n");
    return(1);
}