/*
 * jbolt — Embedded key-value store for Janet, wrapping LMDB.
 *
 * Single C file implementing the complete jbolt API:
 *   open, close, ensure-bucket, drop-bucket, buckets,
 *   put, get, delete, has?,
 *   each, collect, map, filter, keys, count, first, last, seek,
 *   prefix, range,
 *   update, view, tx-put, tx-get, tx-delete,
 *   next-id, backup, stats, db-stats
 */

#include <janet.h>
#include <lmdb.h>
#include <string.h>

/* ----------------------------------------------------------------
 * Constants
 * ---------------------------------------------------------------- */

#define JBOLT_FLAG_CLOSED  1
#define JBOLT_TX_RDONLY    1
#define JBOLT_TX_FINISHED  2

#define JBOLT_DEFAULT_MAX_BUCKETS  16
#define JBOLT_DEFAULT_MAP_SIZE     (256ULL * 1024 * 1024)  /* 256 MB */
#define JBOLT_DEFAULT_MODE         0664

#define JBOLT_SEQ_KEY  "__seq__"

/* ----------------------------------------------------------------
 * Error handling
 * ---------------------------------------------------------------- */

#define JBOLT_CHECK(rc) do { \
    int _rc = (rc); \
    if (_rc != MDB_SUCCESS) { \
        janet_panicf("jbolt: %s", mdb_strerror(_rc)); \
    } \
} while (0)

static void jbolt_check_open(int flags) {
    if (flags & JBOLT_FLAG_CLOSED) {
        janet_panic("jbolt: database is closed");
    }
}

/* ----------------------------------------------------------------
 * JBoltDB — abstract type wrapping MDB_env*
 * ---------------------------------------------------------------- */

typedef struct {
    MDB_env *env;
    JanetTable *dbi_cache;
    int flags;
} JBoltDB;

static int jbolt_db_gc(void *data, size_t len) {
    (void)len;
    JBoltDB *db = (JBoltDB *)data;
    if (db->env && !(db->flags & JBOLT_FLAG_CLOSED)) {
        mdb_env_close(db->env);
        db->env = NULL;
        db->flags |= JBOLT_FLAG_CLOSED;
    }
    return 0;
}

static int jbolt_db_gcmark(void *data, size_t len) {
    (void)len;
    JBoltDB *db = (JBoltDB *)data;
    if (db->dbi_cache) {
        janet_mark(janet_wrap_table(db->dbi_cache));
    }
    return 0;
}

static const JanetAbstractType jbolt_db_type = {
    "jbolt.db",
    jbolt_db_gc,
    jbolt_db_gcmark,
    JANET_ATEND_GCMARK
};

/* ----------------------------------------------------------------
 * JBoltTx — abstract type wrapping MDB_txn*
 * ---------------------------------------------------------------- */

typedef struct {
    MDB_txn *txn;
    JBoltDB *db;
    int flags;
} JBoltTx;

static int jbolt_tx_gcmark(void *data, size_t len) {
    (void)len;
    JBoltTx *tx = (JBoltTx *)data;
    if (tx->db) {
        janet_mark(janet_wrap_abstract(tx->db));
    }
    return 0;
}

static const JanetAbstractType jbolt_tx_type = {
    "jbolt.tx",
    NULL,            /* no gc — txn is always committed/aborted explicitly */
    jbolt_tx_gcmark,
    JANET_ATEND_GCMARK
};

/* ----------------------------------------------------------------
 * DBI cache helper
 * ---------------------------------------------------------------- */

static MDB_dbi jbolt_open_dbi(JBoltDB *db, MDB_txn *txn,
                               const char *bucket, unsigned int flags) {
    Janet key = janet_cstringv(bucket);
    Janet cached = janet_table_get(db->dbi_cache, key);
    if (!janet_checktype(cached, JANET_NIL)) {
        return (MDB_dbi)janet_unwrap_integer(cached);
    }
    MDB_dbi dbi;
    int rc = mdb_dbi_open(txn, bucket, flags, &dbi);
    if (rc == MDB_NOTFOUND) {
        janet_panicf("jbolt: bucket not found: %s", bucket);
    }
    JBOLT_CHECK(rc);
    janet_table_put(db->dbi_cache, key, janet_wrap_integer((int32_t)dbi));
    return dbi;
}

/* ----------------------------------------------------------------
 * Marshal / Unmarshal helpers
 * ---------------------------------------------------------------- */

static void jbolt_marshal(JanetBuffer *buf, Janet value) {
    buf->count = 0;
    janet_marshal(buf, value, NULL, 0);
}

static Janet jbolt_unmarshal(const uint8_t *bytes, size_t len) {
    return janet_unmarshal(bytes, len, 0, NULL, NULL);
}

/* ----------------------------------------------------------------
 * Phase 1: DB lifecycle
 * ---------------------------------------------------------------- */

static Janet jbolt_open(int32_t argc, Janet *argv) {
    janet_arity(argc, 1, -1);
    const char *path = (const char *)janet_getstring(argv, 0);

    int max_buckets = JBOLT_DEFAULT_MAX_BUCKETS;
    size_t map_size = JBOLT_DEFAULT_MAP_SIZE;
    int mode = JBOLT_DEFAULT_MODE;

    /* Parse keyword args */
    for (int32_t i = 1; i < argc; i += 2) {
        if (i + 1 >= argc) janet_panic("jbolt: expected value after keyword");
        if (janet_keyeq(argv[i], "max-buckets")) {
            max_buckets = janet_getinteger(argv, i + 1);
        } else if (janet_keyeq(argv[i], "map-size")) {
            map_size = (size_t)janet_getinteger64(argv, i + 1);
        } else if (janet_keyeq(argv[i], "mode")) {
            mode = janet_getinteger(argv, i + 1);
        } else {
            janet_panicf("jbolt: unknown option %v", argv[i]);
        }
    }

    MDB_env *env;
    JBOLT_CHECK(mdb_env_create(&env));
    JBOLT_CHECK(mdb_env_set_maxdbs(env, max_buckets));
    JBOLT_CHECK(mdb_env_set_mapsize(env, map_size));

    int rc = mdb_env_open(env, path, MDB_NOSUBDIR, (mdb_mode_t)mode);
    if (rc != MDB_SUCCESS) {
        mdb_env_close(env);
        janet_panicf("jbolt: %s", mdb_strerror(rc));
    }

    JBoltDB *db = janet_abstract(&jbolt_db_type, sizeof(JBoltDB));
    db->env = env;
    db->dbi_cache = janet_table(8);
    db->flags = 0;
    return janet_wrap_abstract(db);
}

static Janet jbolt_close(int32_t argc, Janet *argv) {
    janet_fixarity(argc, 1);
    JBoltDB *db = janet_getabstract(argv, 0, &jbolt_db_type);
    if (!(db->flags & JBOLT_FLAG_CLOSED) && db->env) {
        mdb_env_close(db->env);
        db->env = NULL;
        db->flags |= JBOLT_FLAG_CLOSED;
    }
    return janet_wrap_nil();
}

/* ----------------------------------------------------------------
 * Phase 1: Bucket management
 * ---------------------------------------------------------------- */

static Janet jbolt_ensure_bucket(int32_t argc, Janet *argv) {
    janet_fixarity(argc, 2);
    JBoltDB *db = janet_getabstract(argv, 0, &jbolt_db_type);
    jbolt_check_open(db->flags);
    const char *name = (const char *)janet_getstring(argv, 1);

    MDB_txn *txn;
    JBOLT_CHECK(mdb_txn_begin(db->env, NULL, 0, &txn));
    MDB_dbi dbi;
    int rc = mdb_dbi_open(txn, name, MDB_CREATE, &dbi);
    if (rc != MDB_SUCCESS) {
        mdb_txn_abort(txn);
        janet_panicf("jbolt: %s", mdb_strerror(rc));
    }
    /* Cache the dbi */
    janet_table_put(db->dbi_cache, janet_cstringv(name),
                    janet_wrap_integer((int32_t)dbi));
    JBOLT_CHECK(mdb_txn_commit(txn));
    return janet_wrap_nil();
}

static Janet jbolt_drop_bucket(int32_t argc, Janet *argv) {
    janet_fixarity(argc, 2);
    JBoltDB *db = janet_getabstract(argv, 0, &jbolt_db_type);
    jbolt_check_open(db->flags);
    const char *name = (const char *)janet_getstring(argv, 1);

    MDB_txn *txn;
    JBOLT_CHECK(mdb_txn_begin(db->env, NULL, 0, &txn));
    MDB_dbi dbi;
    int rc = mdb_dbi_open(txn, name, 0, &dbi);
    if (rc == MDB_NOTFOUND) {
        mdb_txn_abort(txn);
        janet_panicf("jbolt: bucket not found: %s", name);
    }
    if (rc != MDB_SUCCESS) {
        mdb_txn_abort(txn);
        janet_panicf("jbolt: %s", mdb_strerror(rc));
    }
    rc = mdb_drop(txn, dbi, 1);
    if (rc != MDB_SUCCESS) {
        mdb_txn_abort(txn);
        janet_panicf("jbolt: %s", mdb_strerror(rc));
    }
    /* Remove from cache */
    janet_table_put(db->dbi_cache, janet_cstringv(name), janet_wrap_nil());
    JBOLT_CHECK(mdb_txn_commit(txn));
    return janet_wrap_nil();
}

static Janet jbolt_buckets(int32_t argc, Janet *argv) {
    janet_fixarity(argc, 1);
    JBoltDB *db = janet_getabstract(argv, 0, &jbolt_db_type);
    jbolt_check_open(db->flags);

    MDB_txn *txn;
    JBOLT_CHECK(mdb_txn_begin(db->env, NULL, MDB_RDONLY, &txn));

    MDB_dbi dbi;
    int rc = mdb_dbi_open(txn, NULL, 0, &dbi);
    if (rc != MDB_SUCCESS) {
        mdb_txn_abort(txn);
        janet_panicf("jbolt: %s", mdb_strerror(rc));
    }

    MDB_cursor *cursor;
    rc = mdb_cursor_open(txn, dbi, &cursor);
    if (rc != MDB_SUCCESS) {
        mdb_txn_abort(txn);
        janet_panicf("jbolt: %s", mdb_strerror(rc));
    }

    JanetArray *arr = janet_array(8);
    MDB_val key, val;
    while ((rc = mdb_cursor_get(cursor, &key, &val, MDB_NEXT)) == MDB_SUCCESS) {
        janet_array_push(arr, janet_stringv(key.mv_data, key.mv_size));
    }

    mdb_cursor_close(cursor);
    mdb_txn_abort(txn);
    return janet_wrap_array(arr);
}

/* ----------------------------------------------------------------
 * Phase 1: CRUD
 * ---------------------------------------------------------------- */

static Janet jbolt_put(int32_t argc, Janet *argv) {
    janet_fixarity(argc, 4);
    JBoltDB *db = janet_getabstract(argv, 0, &jbolt_db_type);
    jbolt_check_open(db->flags);
    const char *bucket = (const char *)janet_getstring(argv, 1);
    const char *keystr = (const char *)janet_getstring(argv, 2);
    Janet value = argv[3];

    JanetBuffer *buf = janet_buffer(64);
    jbolt_marshal(buf, value);

    MDB_txn *txn;
    JBOLT_CHECK(mdb_txn_begin(db->env, NULL, 0, &txn));

    MDB_dbi dbi;
    int rc = mdb_dbi_open(txn, bucket, MDB_CREATE, &dbi);
    if (rc != MDB_SUCCESS) {
        mdb_txn_abort(txn);
        janet_panicf("jbolt: %s", mdb_strerror(rc));
    }
    /* Cache */
    janet_table_put(db->dbi_cache, janet_cstringv(bucket),
                    janet_wrap_integer((int32_t)dbi));

    MDB_val mkey = {strlen(keystr), (void *)keystr};
    MDB_val mval = {buf->count, buf->data};

    rc = mdb_put(txn, dbi, &mkey, &mval, 0);
    if (rc != MDB_SUCCESS) {
        mdb_txn_abort(txn);
        janet_panicf("jbolt: %s", mdb_strerror(rc));
    }

    JBOLT_CHECK(mdb_txn_commit(txn));
    return janet_wrap_nil();
}

static Janet jbolt_get(int32_t argc, Janet *argv) {
    janet_fixarity(argc, 3);
    JBoltDB *db = janet_getabstract(argv, 0, &jbolt_db_type);
    jbolt_check_open(db->flags);
    const char *bucket = (const char *)janet_getstring(argv, 1);
    const char *keystr = (const char *)janet_getstring(argv, 2);

    MDB_txn *txn;
    JBOLT_CHECK(mdb_txn_begin(db->env, NULL, MDB_RDONLY, &txn));

    MDB_dbi dbi;
    int rc = mdb_dbi_open(txn, bucket, 0, &dbi);
    if (rc == MDB_NOTFOUND) {
        mdb_txn_abort(txn);
        return janet_wrap_nil();
    }
    if (rc != MDB_SUCCESS) {
        mdb_txn_abort(txn);
        janet_panicf("jbolt: %s", mdb_strerror(rc));
    }
    janet_table_put(db->dbi_cache, janet_cstringv(bucket),
                    janet_wrap_integer((int32_t)dbi));

    MDB_val mkey = {strlen(keystr), (void *)keystr};
    MDB_val mval;

    rc = mdb_get(txn, dbi, &mkey, &mval);
    if (rc == MDB_NOTFOUND) {
        mdb_txn_abort(txn);
        return janet_wrap_nil();
    }
    if (rc != MDB_SUCCESS) {
        mdb_txn_abort(txn);
        janet_panicf("jbolt: %s", mdb_strerror(rc));
    }

    Janet result = jbolt_unmarshal(mval.mv_data, mval.mv_size);
    mdb_txn_abort(txn);
    return result;
}

static Janet jbolt_delete(int32_t argc, Janet *argv) {
    janet_fixarity(argc, 3);
    JBoltDB *db = janet_getabstract(argv, 0, &jbolt_db_type);
    jbolt_check_open(db->flags);
    const char *bucket = (const char *)janet_getstring(argv, 1);
    const char *keystr = (const char *)janet_getstring(argv, 2);

    MDB_txn *txn;
    JBOLT_CHECK(mdb_txn_begin(db->env, NULL, 0, &txn));

    MDB_dbi dbi;
    int rc = mdb_dbi_open(txn, bucket, 0, &dbi);
    if (rc == MDB_NOTFOUND) {
        mdb_txn_abort(txn);
        janet_panicf("jbolt: bucket not found: %s", bucket);
    }
    if (rc != MDB_SUCCESS) {
        mdb_txn_abort(txn);
        janet_panicf("jbolt: %s", mdb_strerror(rc));
    }
    janet_table_put(db->dbi_cache, janet_cstringv(bucket),
                    janet_wrap_integer((int32_t)dbi));

    MDB_val mkey = {strlen(keystr), (void *)keystr};
    rc = mdb_del(txn, dbi, &mkey, NULL);
    if (rc != MDB_SUCCESS && rc != MDB_NOTFOUND) {
        mdb_txn_abort(txn);
        janet_panicf("jbolt: %s", mdb_strerror(rc));
    }

    JBOLT_CHECK(mdb_txn_commit(txn));
    return janet_wrap_nil();
}

static Janet jbolt_has(int32_t argc, Janet *argv) {
    janet_fixarity(argc, 3);
    JBoltDB *db = janet_getabstract(argv, 0, &jbolt_db_type);
    jbolt_check_open(db->flags);
    const char *bucket = (const char *)janet_getstring(argv, 1);
    const char *keystr = (const char *)janet_getstring(argv, 2);

    MDB_txn *txn;
    JBOLT_CHECK(mdb_txn_begin(db->env, NULL, MDB_RDONLY, &txn));

    MDB_dbi dbi;
    int rc = mdb_dbi_open(txn, bucket, 0, &dbi);
    if (rc == MDB_NOTFOUND) {
        mdb_txn_abort(txn);
        return janet_wrap_boolean(0);
    }
    if (rc != MDB_SUCCESS) {
        mdb_txn_abort(txn);
        janet_panicf("jbolt: %s", mdb_strerror(rc));
    }
    janet_table_put(db->dbi_cache, janet_cstringv(bucket),
                    janet_wrap_integer((int32_t)dbi));

    MDB_val mkey = {strlen(keystr), (void *)keystr};
    MDB_val mval;
    rc = mdb_get(txn, dbi, &mkey, &mval);
    mdb_txn_abort(txn);
    return janet_wrap_boolean(rc == MDB_SUCCESS);
}

static Janet jbolt_merge(int32_t argc, Janet *argv) {
    janet_fixarity(argc, 4);
    JBoltDB *db = janet_getabstract(argv, 0, &jbolt_db_type);
    jbolt_check_open(db->flags);
    const char *bucket = (const char *)janet_getstring(argv, 1);
    const char *keystr = (const char *)janet_getstring(argv, 2);
    Janet updates = argv[3];

    MDB_txn *txn;
    JBOLT_CHECK(mdb_txn_begin(db->env, NULL, 0, &txn));

    MDB_dbi dbi;
    int rc = mdb_dbi_open(txn, bucket, MDB_CREATE, &dbi);
    if (rc != MDB_SUCCESS) {
        mdb_txn_abort(txn);
        janet_panicf("jbolt: %s", mdb_strerror(rc));
    }
    janet_table_put(db->dbi_cache, janet_cstringv(bucket),
                    janet_wrap_integer((int32_t)dbi));

    /* Read existing value */
    MDB_val mkey = {strlen(keystr), (void *)keystr};
    MDB_val mval;
    JanetTable *tbl;

    rc = mdb_get(txn, dbi, &mkey, &mval);
    if (rc == MDB_SUCCESS) {
        Janet existing = jbolt_unmarshal(mval.mv_data, mval.mv_size);
        /* Convert struct to table if needed */
        if (janet_checktype(existing, JANET_STRUCT)) {
            tbl = janet_struct_to_table(janet_unwrap_struct(existing));
        } else if (janet_checktype(existing, JANET_TABLE)) {
            tbl = janet_unwrap_table(existing);
        } else {
            mdb_txn_abort(txn);
            janet_panic("jbolt: merge requires existing value to be a table or struct");
        }
    } else if (rc == MDB_NOTFOUND) {
        tbl = janet_table(8);
    } else {
        mdb_txn_abort(txn);
        janet_panicf("jbolt: %s", mdb_strerror(rc));
    }

    /* Merge updates — nil values remove keys */
    const JanetKV *kvs;
    int32_t cap;
    if (janet_checktype(updates, JANET_STRUCT)) {
        JanetStruct st = janet_unwrap_struct(updates);
        kvs = st;
        cap = janet_struct_capacity(st);
    } else if (janet_checktype(updates, JANET_TABLE)) {
        JanetTable *upd = janet_unwrap_table(updates);
        kvs = upd->data;
        cap = upd->capacity;
    } else {
        mdb_txn_abort(txn);
        janet_panic("jbolt: merge updates must be a table or struct");
        return janet_wrap_nil(); /* unreachable */
    }
    const JanetKV *kv = NULL;
    while ((kv = janet_dictionary_next(kvs, cap, kv))) {
        janet_table_put(tbl, kv->key, kv->value);
    }

    /* Write back */
    JanetBuffer *buf = janet_buffer(64);
    jbolt_marshal(buf, janet_wrap_table(tbl));
    MDB_val new_val = {buf->count, buf->data};
    rc = mdb_put(txn, dbi, &mkey, &new_val, 0);
    if (rc != MDB_SUCCESS) {
        mdb_txn_abort(txn);
        janet_panicf("jbolt: %s", mdb_strerror(rc));
    }

    JBOLT_CHECK(mdb_txn_commit(txn));
    return janet_wrap_table(tbl);
}

static Janet jbolt_dissoc(int32_t argc, Janet *argv) {
    janet_arity(argc, 4, -1);
    JBoltDB *db = janet_getabstract(argv, 0, &jbolt_db_type);
    jbolt_check_open(db->flags);
    const char *bucket = (const char *)janet_getstring(argv, 1);
    const char *keystr = (const char *)janet_getstring(argv, 2);

    MDB_txn *txn;
    JBOLT_CHECK(mdb_txn_begin(db->env, NULL, 0, &txn));

    MDB_dbi dbi;
    int rc = mdb_dbi_open(txn, bucket, 0, &dbi);
    if (rc == MDB_NOTFOUND) {
        mdb_txn_abort(txn);
        janet_panicf("jbolt: bucket not found: %s", bucket);
    }
    if (rc != MDB_SUCCESS) {
        mdb_txn_abort(txn);
        janet_panicf("jbolt: %s", mdb_strerror(rc));
    }
    janet_table_put(db->dbi_cache, janet_cstringv(bucket),
                    janet_wrap_integer((int32_t)dbi));

    MDB_val mkey = {strlen(keystr), (void *)keystr};
    MDB_val mval;

    rc = mdb_get(txn, dbi, &mkey, &mval);
    if (rc == MDB_NOTFOUND) {
        mdb_txn_abort(txn);
        return janet_wrap_nil();
    }
    if (rc != MDB_SUCCESS) {
        mdb_txn_abort(txn);
        janet_panicf("jbolt: %s", mdb_strerror(rc));
    }

    Janet existing = jbolt_unmarshal(mval.mv_data, mval.mv_size);
    JanetTable *tbl;
    if (janet_checktype(existing, JANET_STRUCT)) {
        tbl = janet_struct_to_table(janet_unwrap_struct(existing));
    } else if (janet_checktype(existing, JANET_TABLE)) {
        tbl = janet_unwrap_table(existing);
    } else {
        mdb_txn_abort(txn);
        janet_panic("jbolt: dissoc requires value to be a table or struct");
        return janet_wrap_nil();
    }

    /* Remove specified keys */
    for (int32_t i = 3; i < argc; i++) {
        janet_table_put(tbl, argv[i], janet_wrap_nil());
    }

    /* Write back */
    JanetBuffer *buf = janet_buffer(64);
    jbolt_marshal(buf, janet_wrap_table(tbl));
    MDB_val new_val = {buf->count, buf->data};
    rc = mdb_put(txn, dbi, &mkey, &new_val, 0);
    if (rc != MDB_SUCCESS) {
        mdb_txn_abort(txn);
        janet_panicf("jbolt: %s", mdb_strerror(rc));
    }

    JBOLT_CHECK(mdb_txn_commit(txn));
    return janet_wrap_table(tbl);
}

/* ----------------------------------------------------------------
 * Phase 2: Iteration helpers
 * ---------------------------------------------------------------- */

/* Open a read-only txn + dbi + cursor for iteration.
 * Returns 0 on success, -1 if bucket not found (sets *txn_out=NULL). */
static int jbolt_iter_begin(JBoltDB *db, const char *bucket,
                            MDB_txn **txn_out, MDB_dbi *dbi_out,
                            MDB_cursor **cur_out) {
    MDB_txn *txn;
    JBOLT_CHECK(mdb_txn_begin(db->env, NULL, MDB_RDONLY, &txn));

    MDB_dbi dbi;
    int rc = mdb_dbi_open(txn, bucket, 0, &dbi);
    if (rc == MDB_NOTFOUND) {
        mdb_txn_abort(txn);
        *txn_out = NULL;
        return -1;
    }
    if (rc != MDB_SUCCESS) {
        mdb_txn_abort(txn);
        janet_panicf("jbolt: %s", mdb_strerror(rc));
    }
    janet_table_put(db->dbi_cache, janet_cstringv(bucket),
                    janet_wrap_integer((int32_t)dbi));

    MDB_cursor *cursor;
    rc = mdb_cursor_open(txn, dbi, &cursor);
    if (rc != MDB_SUCCESS) {
        mdb_txn_abort(txn);
        janet_panicf("jbolt: %s", mdb_strerror(rc));
    }

    *txn_out = txn;
    *dbi_out = dbi;
    *cur_out = cursor;
    return 0;
}

static void jbolt_iter_end(MDB_cursor *cursor, MDB_txn *txn) {
    mdb_cursor_close(cursor);
    mdb_txn_abort(txn);
}

/* Call a Janet function safely within a cursor iteration.
 * On error, cleans up and re-raises. */
static JanetSignal jbolt_call_safe(JanetFunction *fn, int32_t argn,
                                    const Janet *argv, Janet *out,
                                    MDB_cursor *cursor, MDB_txn *txn) {
    JanetSignal sig = janet_pcall(fn, argn, argv, out, NULL);
    if (sig != JANET_SIGNAL_OK) {
        jbolt_iter_end(cursor, txn);
        janet_panicv(*out);
    }
    return sig;
}

/* ----------------------------------------------------------------
 * Phase 2: each, collect, map, filter
 * ---------------------------------------------------------------- */

static Janet jbolt_each(int32_t argc, Janet *argv) {
    janet_fixarity(argc, 3);
    JBoltDB *db = janet_getabstract(argv, 0, &jbolt_db_type);
    jbolt_check_open(db->flags);
    const char *bucket = (const char *)janet_getstring(argv, 1);
    JanetFunction *fn = janet_getfunction(argv, 2);

    MDB_txn *txn; MDB_dbi dbi; MDB_cursor *cursor;
    if (jbolt_iter_begin(db, bucket, &txn, &dbi, &cursor) < 0) {
        return janet_wrap_nil();
    }

    MDB_val key, val;
    int rc;
    while ((rc = mdb_cursor_get(cursor, &key, &val, MDB_NEXT)) == MDB_SUCCESS) {
        Janet args[2];
        args[0] = janet_stringv(key.mv_data, key.mv_size);
        args[1] = jbolt_unmarshal(val.mv_data, val.mv_size);
        Janet out;
        jbolt_call_safe(fn, 2, args, &out, cursor, txn);
    }

    jbolt_iter_end(cursor, txn);
    return janet_wrap_nil();
}

static Janet jbolt_collect(int32_t argc, Janet *argv) {
    janet_fixarity(argc, 2);
    JBoltDB *db = janet_getabstract(argv, 0, &jbolt_db_type);
    jbolt_check_open(db->flags);
    const char *bucket = (const char *)janet_getstring(argv, 1);

    MDB_txn *txn; MDB_dbi dbi; MDB_cursor *cursor;
    if (jbolt_iter_begin(db, bucket, &txn, &dbi, &cursor) < 0) {
        return janet_wrap_array(janet_array(0));
    }

    JanetArray *arr = janet_array(16);
    MDB_val key, val;
    int rc;
    while ((rc = mdb_cursor_get(cursor, &key, &val, MDB_NEXT)) == MDB_SUCCESS) {
        Janet tuple[2];
        tuple[0] = janet_stringv(key.mv_data, key.mv_size);
        tuple[1] = jbolt_unmarshal(val.mv_data, val.mv_size);
        janet_array_push(arr, janet_wrap_tuple(janet_tuple_n(tuple, 2)));
    }

    jbolt_iter_end(cursor, txn);
    return janet_wrap_array(arr);
}

static Janet jbolt_map(int32_t argc, Janet *argv) {
    janet_fixarity(argc, 3);
    JBoltDB *db = janet_getabstract(argv, 0, &jbolt_db_type);
    jbolt_check_open(db->flags);
    const char *bucket = (const char *)janet_getstring(argv, 1);
    JanetFunction *fn = janet_getfunction(argv, 2);

    MDB_txn *txn; MDB_dbi dbi; MDB_cursor *cursor;
    if (jbolt_iter_begin(db, bucket, &txn, &dbi, &cursor) < 0) {
        return janet_wrap_array(janet_array(0));
    }

    JanetArray *arr = janet_array(16);
    MDB_val key, val;
    int rc;
    while ((rc = mdb_cursor_get(cursor, &key, &val, MDB_NEXT)) == MDB_SUCCESS) {
        Janet args[2];
        args[0] = janet_stringv(key.mv_data, key.mv_size);
        args[1] = jbolt_unmarshal(val.mv_data, val.mv_size);
        Janet out;
        jbolt_call_safe(fn, 2, args, &out, cursor, txn);
        janet_array_push(arr, out);
    }

    jbolt_iter_end(cursor, txn);
    return janet_wrap_array(arr);
}

static Janet jbolt_filter(int32_t argc, Janet *argv) {
    janet_fixarity(argc, 3);
    JBoltDB *db = janet_getabstract(argv, 0, &jbolt_db_type);
    jbolt_check_open(db->flags);
    const char *bucket = (const char *)janet_getstring(argv, 1);
    JanetFunction *fn = janet_getfunction(argv, 2);

    MDB_txn *txn; MDB_dbi dbi; MDB_cursor *cursor;
    if (jbolt_iter_begin(db, bucket, &txn, &dbi, &cursor) < 0) {
        return janet_wrap_array(janet_array(0));
    }

    JanetArray *arr = janet_array(16);
    MDB_val key, val;
    int rc;
    while ((rc = mdb_cursor_get(cursor, &key, &val, MDB_NEXT)) == MDB_SUCCESS) {
        Janet args[2];
        args[0] = janet_stringv(key.mv_data, key.mv_size);
        args[1] = jbolt_unmarshal(val.mv_data, val.mv_size);
        Janet out;
        jbolt_call_safe(fn, 2, args, &out, cursor, txn);
        if (janet_truthy(out)) {
            Janet tuple[2];
            tuple[0] = args[0];
            tuple[1] = args[1];
            janet_array_push(arr, janet_wrap_tuple(janet_tuple_n(tuple, 2)));
        }
    }

    jbolt_iter_end(cursor, txn);
    return janet_wrap_array(arr);
}

/* ----------------------------------------------------------------
 * Phase 2: keys, count, first, last, seek
 * ---------------------------------------------------------------- */

static Janet jbolt_keys(int32_t argc, Janet *argv) {
    janet_fixarity(argc, 2);
    JBoltDB *db = janet_getabstract(argv, 0, &jbolt_db_type);
    jbolt_check_open(db->flags);
    const char *bucket = (const char *)janet_getstring(argv, 1);

    MDB_txn *txn; MDB_dbi dbi; MDB_cursor *cursor;
    if (jbolt_iter_begin(db, bucket, &txn, &dbi, &cursor) < 0) {
        return janet_wrap_array(janet_array(0));
    }

    JanetArray *arr = janet_array(16);
    MDB_val key, val;
    int rc;
    while ((rc = mdb_cursor_get(cursor, &key, &val, MDB_NEXT)) == MDB_SUCCESS) {
        janet_array_push(arr, janet_stringv(key.mv_data, key.mv_size));
    }

    jbolt_iter_end(cursor, txn);
    return janet_wrap_array(arr);
}

static Janet jbolt_count(int32_t argc, Janet *argv) {
    janet_fixarity(argc, 2);
    JBoltDB *db = janet_getabstract(argv, 0, &jbolt_db_type);
    jbolt_check_open(db->flags);
    const char *bucket = (const char *)janet_getstring(argv, 1);

    MDB_txn *txn;
    JBOLT_CHECK(mdb_txn_begin(db->env, NULL, MDB_RDONLY, &txn));

    MDB_dbi dbi;
    int rc = mdb_dbi_open(txn, bucket, 0, &dbi);
    if (rc == MDB_NOTFOUND) {
        mdb_txn_abort(txn);
        return janet_wrap_integer(0);
    }
    if (rc != MDB_SUCCESS) {
        mdb_txn_abort(txn);
        janet_panicf("jbolt: %s", mdb_strerror(rc));
    }
    janet_table_put(db->dbi_cache, janet_cstringv(bucket),
                    janet_wrap_integer((int32_t)dbi));

    MDB_stat stat;
    rc = mdb_stat(txn, dbi, &stat);
    mdb_txn_abort(txn);
    if (rc != MDB_SUCCESS) {
        janet_panicf("jbolt: %s", mdb_strerror(rc));
    }

    return janet_wrap_number((double)stat.ms_entries);
}

static Janet jbolt_first(int32_t argc, Janet *argv) {
    janet_fixarity(argc, 2);
    JBoltDB *db = janet_getabstract(argv, 0, &jbolt_db_type);
    jbolt_check_open(db->flags);
    const char *bucket = (const char *)janet_getstring(argv, 1);

    MDB_txn *txn; MDB_dbi dbi; MDB_cursor *cursor;
    if (jbolt_iter_begin(db, bucket, &txn, &dbi, &cursor) < 0) {
        return janet_wrap_nil();
    }

    MDB_val key, val;
    int rc = mdb_cursor_get(cursor, &key, &val, MDB_FIRST);
    Janet result;
    if (rc == MDB_SUCCESS) {
        Janet tuple[2];
        tuple[0] = janet_stringv(key.mv_data, key.mv_size);
        tuple[1] = jbolt_unmarshal(val.mv_data, val.mv_size);
        result = janet_wrap_tuple(janet_tuple_n(tuple, 2));
    } else {
        result = janet_wrap_nil();
    }

    jbolt_iter_end(cursor, txn);
    return result;
}

static Janet jbolt_last(int32_t argc, Janet *argv) {
    janet_fixarity(argc, 2);
    JBoltDB *db = janet_getabstract(argv, 0, &jbolt_db_type);
    jbolt_check_open(db->flags);
    const char *bucket = (const char *)janet_getstring(argv, 1);

    MDB_txn *txn; MDB_dbi dbi; MDB_cursor *cursor;
    if (jbolt_iter_begin(db, bucket, &txn, &dbi, &cursor) < 0) {
        return janet_wrap_nil();
    }

    MDB_val key, val;
    int rc = mdb_cursor_get(cursor, &key, &val, MDB_LAST);
    Janet result;
    if (rc == MDB_SUCCESS) {
        Janet tuple[2];
        tuple[0] = janet_stringv(key.mv_data, key.mv_size);
        tuple[1] = jbolt_unmarshal(val.mv_data, val.mv_size);
        result = janet_wrap_tuple(janet_tuple_n(tuple, 2));
    } else {
        result = janet_wrap_nil();
    }

    jbolt_iter_end(cursor, txn);
    return result;
}

static Janet jbolt_seek(int32_t argc, Janet *argv) {
    janet_fixarity(argc, 3);
    JBoltDB *db = janet_getabstract(argv, 0, &jbolt_db_type);
    jbolt_check_open(db->flags);
    const char *bucket = (const char *)janet_getstring(argv, 1);
    const char *keystr = (const char *)janet_getstring(argv, 2);

    MDB_txn *txn; MDB_dbi dbi; MDB_cursor *cursor;
    if (jbolt_iter_begin(db, bucket, &txn, &dbi, &cursor) < 0) {
        return janet_wrap_nil();
    }

    MDB_val key = {strlen(keystr), (void *)keystr};
    MDB_val val;
    int rc = mdb_cursor_get(cursor, &key, &val, MDB_SET_RANGE);
    Janet result;
    if (rc == MDB_SUCCESS) {
        Janet tuple[2];
        tuple[0] = janet_stringv(key.mv_data, key.mv_size);
        tuple[1] = jbolt_unmarshal(val.mv_data, val.mv_size);
        result = janet_wrap_tuple(janet_tuple_n(tuple, 2));
    } else {
        result = janet_wrap_nil();
    }

    jbolt_iter_end(cursor, txn);
    return result;
}

/* ----------------------------------------------------------------
 * Phase 2: prefix, range
 * ---------------------------------------------------------------- */

static Janet jbolt_prefix(int32_t argc, Janet *argv) {
    janet_fixarity(argc, 4);
    JBoltDB *db = janet_getabstract(argv, 0, &jbolt_db_type);
    jbolt_check_open(db->flags);
    const char *bucket = (const char *)janet_getstring(argv, 1);
    const uint8_t *prefix = janet_getstring(argv, 2);
    int32_t prefix_len = janet_string_length(prefix);
    JanetFunction *fn = janet_getfunction(argv, 3);

    MDB_txn *txn; MDB_dbi dbi; MDB_cursor *cursor;
    if (jbolt_iter_begin(db, bucket, &txn, &dbi, &cursor) < 0) {
        return janet_wrap_nil();
    }

    MDB_val key = {prefix_len, (void *)prefix};
    MDB_val val;
    int rc = mdb_cursor_get(cursor, &key, &val, MDB_SET_RANGE);

    while (rc == MDB_SUCCESS) {
        /* Check if key still starts with prefix */
        if ((int32_t)key.mv_size < prefix_len ||
            memcmp(key.mv_data, prefix, prefix_len) != 0) {
            break;
        }
        Janet args[2];
        args[0] = janet_stringv(key.mv_data, key.mv_size);
        args[1] = jbolt_unmarshal(val.mv_data, val.mv_size);
        Janet out;
        jbolt_call_safe(fn, 2, args, &out, cursor, txn);
        rc = mdb_cursor_get(cursor, &key, &val, MDB_NEXT);
    }

    jbolt_iter_end(cursor, txn);
    return janet_wrap_nil();
}

static Janet jbolt_range(int32_t argc, Janet *argv) {
    janet_fixarity(argc, 5);
    JBoltDB *db = janet_getabstract(argv, 0, &jbolt_db_type);
    jbolt_check_open(db->flags);
    const char *bucket = (const char *)janet_getstring(argv, 1);
    const uint8_t *start = janet_getstring(argv, 2);
    int32_t start_len = janet_string_length(start);
    const uint8_t *end = janet_getstring(argv, 3);
    int32_t end_len = janet_string_length(end);
    JanetFunction *fn = janet_getfunction(argv, 4);

    MDB_txn *txn; MDB_dbi dbi; MDB_cursor *cursor;
    if (jbolt_iter_begin(db, bucket, &txn, &dbi, &cursor) < 0) {
        return janet_wrap_nil();
    }

    MDB_val key = {start_len, (void *)start};
    MDB_val val;
    int rc = mdb_cursor_get(cursor, &key, &val, MDB_SET_RANGE);

    while (rc == MDB_SUCCESS) {
        /* Check if key <= end (byte comparison) */
        int cmp;
        size_t min_len = key.mv_size < (size_t)end_len ? key.mv_size : (size_t)end_len;
        cmp = memcmp(key.mv_data, end, min_len);
        if (cmp > 0 || (cmp == 0 && key.mv_size > (size_t)end_len)) {
            break;
        }

        Janet args[2];
        args[0] = janet_stringv(key.mv_data, key.mv_size);
        args[1] = jbolt_unmarshal(val.mv_data, val.mv_size);
        Janet out;
        jbolt_call_safe(fn, 2, args, &out, cursor, txn);
        rc = mdb_cursor_get(cursor, &key, &val, MDB_NEXT);
    }

    jbolt_iter_end(cursor, txn);
    return janet_wrap_nil();
}

/* ----------------------------------------------------------------
 * Phase 3: Explicit transactions
 * ---------------------------------------------------------------- */

static Janet jbolt_update(int32_t argc, Janet *argv) {
    janet_fixarity(argc, 2);
    JBoltDB *db = janet_getabstract(argv, 0, &jbolt_db_type);
    jbolt_check_open(db->flags);
    JanetFunction *fn = janet_getfunction(argv, 1);

    MDB_txn *txn;
    JBOLT_CHECK(mdb_txn_begin(db->env, NULL, 0, &txn));

    JBoltTx *tx = janet_abstract(&jbolt_tx_type, sizeof(JBoltTx));
    tx->txn = txn;
    tx->db = db;
    tx->flags = 0;

    Janet tx_val = janet_wrap_abstract(tx);
    Janet result;
    JanetSignal sig = janet_pcall(fn, 1, &tx_val, &result, NULL);

    if (sig == JANET_SIGNAL_OK) {
        int rc = mdb_txn_commit(txn);
        tx->txn = NULL;
        tx->flags |= JBOLT_TX_FINISHED;
        JBOLT_CHECK(rc);
        return result;
    } else {
        mdb_txn_abort(txn);
        tx->txn = NULL;
        tx->flags |= JBOLT_TX_FINISHED;
        janet_panicv(result);
        return janet_wrap_nil(); /* unreachable */
    }
}

static Janet jbolt_view(int32_t argc, Janet *argv) {
    janet_fixarity(argc, 2);
    JBoltDB *db = janet_getabstract(argv, 0, &jbolt_db_type);
    jbolt_check_open(db->flags);
    JanetFunction *fn = janet_getfunction(argv, 1);

    MDB_txn *txn;
    JBOLT_CHECK(mdb_txn_begin(db->env, NULL, MDB_RDONLY, &txn));

    JBoltTx *tx = janet_abstract(&jbolt_tx_type, sizeof(JBoltTx));
    tx->txn = txn;
    tx->db = db;
    tx->flags = JBOLT_TX_RDONLY;

    Janet tx_val = janet_wrap_abstract(tx);
    Janet result;
    JanetSignal sig = janet_pcall(fn, 1, &tx_val, &result, NULL);

    mdb_txn_abort(txn);
    tx->txn = NULL;
    tx->flags |= JBOLT_TX_FINISHED;

    if (sig == JANET_SIGNAL_OK) {
        return result;
    } else {
        janet_panicv(result);
        return janet_wrap_nil(); /* unreachable */
    }
}

static void jbolt_check_tx(JBoltTx *tx) {
    if (tx->flags & JBOLT_TX_FINISHED) {
        janet_panic("jbolt: transaction already finished");
    }
}

static Janet jbolt_tx_put(int32_t argc, Janet *argv) {
    janet_fixarity(argc, 4);
    JBoltTx *tx = janet_getabstract(argv, 0, &jbolt_tx_type);
    jbolt_check_tx(tx);
    const char *bucket = (const char *)janet_getstring(argv, 1);
    const char *keystr = (const char *)janet_getstring(argv, 2);
    Janet value = argv[3];

    MDB_dbi dbi = jbolt_open_dbi(tx->db, tx->txn, bucket, MDB_CREATE);

    JanetBuffer *buf = janet_buffer(64);
    jbolt_marshal(buf, value);

    MDB_val mkey = {strlen(keystr), (void *)keystr};
    MDB_val mval = {buf->count, buf->data};

    JBOLT_CHECK(mdb_put(tx->txn, dbi, &mkey, &mval, 0));
    return janet_wrap_nil();
}

static Janet jbolt_tx_get(int32_t argc, Janet *argv) {
    janet_fixarity(argc, 3);
    JBoltTx *tx = janet_getabstract(argv, 0, &jbolt_tx_type);
    jbolt_check_tx(tx);
    const char *bucket = (const char *)janet_getstring(argv, 1);
    const char *keystr = (const char *)janet_getstring(argv, 2);

    /* Try to open dbi without MDB_CREATE */
    Janet cached = janet_table_get(tx->db->dbi_cache, janet_cstringv(bucket));
    MDB_dbi dbi;
    if (!janet_checktype(cached, JANET_NIL)) {
        dbi = (MDB_dbi)janet_unwrap_integer(cached);
    } else {
        int rc = mdb_dbi_open(tx->txn, bucket, 0, &dbi);
        if (rc == MDB_NOTFOUND) return janet_wrap_nil();
        JBOLT_CHECK(rc);
        janet_table_put(tx->db->dbi_cache, janet_cstringv(bucket),
                        janet_wrap_integer((int32_t)dbi));
    }

    MDB_val mkey = {strlen(keystr), (void *)keystr};
    MDB_val mval;
    int rc = mdb_get(tx->txn, dbi, &mkey, &mval);
    if (rc == MDB_NOTFOUND) return janet_wrap_nil();
    JBOLT_CHECK(rc);

    return jbolt_unmarshal(mval.mv_data, mval.mv_size);
}

static Janet jbolt_tx_delete(int32_t argc, Janet *argv) {
    janet_fixarity(argc, 3);
    JBoltTx *tx = janet_getabstract(argv, 0, &jbolt_tx_type);
    jbolt_check_tx(tx);
    const char *bucket = (const char *)janet_getstring(argv, 1);
    const char *keystr = (const char *)janet_getstring(argv, 2);

    MDB_dbi dbi = jbolt_open_dbi(tx->db, tx->txn, bucket, 0);

    MDB_val mkey = {strlen(keystr), (void *)keystr};
    int rc = mdb_del(tx->txn, dbi, &mkey, NULL);
    if (rc != MDB_SUCCESS && rc != MDB_NOTFOUND) {
        janet_panicf("jbolt: %s", mdb_strerror(rc));
    }
    return janet_wrap_nil();
}

/* ----------------------------------------------------------------
 * Phase 4: Utility
 * ---------------------------------------------------------------- */

static Janet jbolt_next_id(int32_t argc, Janet *argv) {
    janet_fixarity(argc, 2);
    JBoltDB *db = janet_getabstract(argv, 0, &jbolt_db_type);
    jbolt_check_open(db->flags);
    const char *bucket = (const char *)janet_getstring(argv, 1);

    MDB_txn *txn;
    JBOLT_CHECK(mdb_txn_begin(db->env, NULL, 0, &txn));

    MDB_dbi dbi;
    int rc = mdb_dbi_open(txn, bucket, MDB_CREATE, &dbi);
    if (rc != MDB_SUCCESS) {
        mdb_txn_abort(txn);
        janet_panicf("jbolt: %s", mdb_strerror(rc));
    }
    janet_table_put(db->dbi_cache, janet_cstringv(bucket),
                    janet_wrap_integer((int32_t)dbi));

    /* Read current sequence */
    MDB_val seq_key = {strlen(JBOLT_SEQ_KEY), (void *)JBOLT_SEQ_KEY};
    MDB_val seq_val;
    int64_t next = 1;

    rc = mdb_get(txn, dbi, &seq_key, &seq_val);
    if (rc == MDB_SUCCESS) {
        Janet current = jbolt_unmarshal(seq_val.mv_data, seq_val.mv_size);
        if (janet_checktype(current, JANET_NUMBER)) {
            next = (int64_t)janet_unwrap_number(current) + 1;
        }
    } else if (rc != MDB_NOTFOUND) {
        mdb_txn_abort(txn);
        janet_panicf("jbolt: %s", mdb_strerror(rc));
    }

    /* Write incremented value */
    JanetBuffer *buf = janet_buffer(16);
    jbolt_marshal(buf, janet_wrap_number((double)next));
    MDB_val new_val = {buf->count, buf->data};
    rc = mdb_put(txn, dbi, &seq_key, &new_val, 0);
    if (rc != MDB_SUCCESS) {
        mdb_txn_abort(txn);
        janet_panicf("jbolt: %s", mdb_strerror(rc));
    }

    JBOLT_CHECK(mdb_txn_commit(txn));
    return janet_wrap_number((double)next);
}

static Janet jbolt_backup(int32_t argc, Janet *argv) {
    janet_fixarity(argc, 2);
    JBoltDB *db = janet_getabstract(argv, 0, &jbolt_db_type);
    jbolt_check_open(db->flags);
    const char *path = (const char *)janet_getstring(argv, 1);

    JBOLT_CHECK(mdb_env_copy2(db->env, path, MDB_CP_COMPACT));
    return janet_wrap_nil();
}

static Janet jbolt_stats(int32_t argc, Janet *argv) {
    janet_fixarity(argc, 2);
    JBoltDB *db = janet_getabstract(argv, 0, &jbolt_db_type);
    jbolt_check_open(db->flags);
    const char *bucket = (const char *)janet_getstring(argv, 1);

    MDB_txn *txn;
    JBOLT_CHECK(mdb_txn_begin(db->env, NULL, MDB_RDONLY, &txn));

    MDB_dbi dbi;
    int rc = mdb_dbi_open(txn, bucket, 0, &dbi);
    if (rc == MDB_NOTFOUND) {
        mdb_txn_abort(txn);
        janet_panicf("jbolt: bucket not found: %s", bucket);
    }
    if (rc != MDB_SUCCESS) {
        mdb_txn_abort(txn);
        janet_panicf("jbolt: %s", mdb_strerror(rc));
    }
    janet_table_put(db->dbi_cache, janet_cstringv(bucket),
                    janet_wrap_integer((int32_t)dbi));

    MDB_stat stat;
    rc = mdb_stat(txn, dbi, &stat);
    mdb_txn_abort(txn);
    if (rc != MDB_SUCCESS) {
        janet_panicf("jbolt: %s", mdb_strerror(rc));
    }

    JanetKV *st = janet_struct_begin(6);
    janet_struct_put(st, janet_ckeywordv("page-size"), janet_wrap_number(stat.ms_psize));
    janet_struct_put(st, janet_ckeywordv("depth"), janet_wrap_number(stat.ms_depth));
    janet_struct_put(st, janet_ckeywordv("branch-pages"), janet_wrap_number(stat.ms_branch_pages));
    janet_struct_put(st, janet_ckeywordv("leaf-pages"), janet_wrap_number(stat.ms_leaf_pages));
    janet_struct_put(st, janet_ckeywordv("overflow-pages"), janet_wrap_number(stat.ms_overflow_pages));
    janet_struct_put(st, janet_ckeywordv("entries"), janet_wrap_number(stat.ms_entries));
    return janet_wrap_struct(janet_struct_end(st));
}

static Janet jbolt_db_stats(int32_t argc, Janet *argv) {
    janet_fixarity(argc, 1);
    JBoltDB *db = janet_getabstract(argv, 0, &jbolt_db_type);
    jbolt_check_open(db->flags);

    MDB_stat stat;
    JBOLT_CHECK(mdb_env_stat(db->env, &stat));

    MDB_envinfo info;
    JBOLT_CHECK(mdb_env_info(db->env, &info));

    JanetKV *st = janet_struct_begin(8);
    janet_struct_put(st, janet_ckeywordv("page-size"), janet_wrap_number(stat.ms_psize));
    janet_struct_put(st, janet_ckeywordv("depth"), janet_wrap_number(stat.ms_depth));
    janet_struct_put(st, janet_ckeywordv("entries"), janet_wrap_number(stat.ms_entries));
    janet_struct_put(st, janet_ckeywordv("map-size"), janet_wrap_number((double)info.me_mapsize));
    janet_struct_put(st, janet_ckeywordv("last-page"), janet_wrap_number((double)info.me_last_pgno));
    janet_struct_put(st, janet_ckeywordv("last-txn"), janet_wrap_number((double)info.me_last_txnid));
    janet_struct_put(st, janet_ckeywordv("max-readers"), janet_wrap_number(info.me_maxreaders));
    janet_struct_put(st, janet_ckeywordv("num-readers"), janet_wrap_number(info.me_numreaders));
    return janet_wrap_struct(janet_struct_end(st));
}

/* ----------------------------------------------------------------
 * Module registration
 * ---------------------------------------------------------------- */

static const JanetReg cfuns[] = {
    {"open", jbolt_open,
     "(jbolt/open path &named :max-buckets :map-size :mode)\n\n"
     "Open or create a database at `path`. Returns a database handle.\n"
     "Optional named parameters:\n"
     "  :max-buckets  Maximum number of buckets (default 16)\n"
     "  :map-size     Maximum database size in bytes (default 256MB)\n"
     "  :mode         File permission bits (default 0664)"},
    {"close", jbolt_close,
     "(jbolt/close db)\n\n"
     "Close the database. Safe to call multiple times."},
    {"ensure-bucket", jbolt_ensure_bucket,
     "(jbolt/ensure-bucket db name)\n\n"
     "Create a bucket if it does not already exist. Idempotent."},
    {"drop-bucket", jbolt_drop_bucket,
     "(jbolt/drop-bucket db name)\n\n"
     "Delete a bucket and all its contents. Raises an error if the bucket does not exist."},
    {"buckets", jbolt_buckets,
     "(jbolt/buckets db)\n\n"
     "Return an array of all bucket names in the database."},
    {"put", jbolt_put,
     "(jbolt/put db bucket key value)\n\n"
     "Store a value in a bucket. Key must be a string, value can be any Janet value. "
     "Creates the bucket if it does not exist. Overwrites any existing value for the key."},
    {"get", jbolt_get,
     "(jbolt/get db bucket key)\n\n"
     "Retrieve a value by key. Returns nil if the bucket or key does not exist."},
    {"delete", jbolt_delete,
     "(jbolt/delete db bucket key)\n\n"
     "Delete a key from a bucket. No error if the key does not exist."},
    {"has?", jbolt_has,
     "(jbolt/has? db bucket key)\n\n"
     "Check if a key exists in a bucket. Returns true or false."},
    {"merge", jbolt_merge,
     "(jbolt/merge db bucket key updates)\n\n"
     "Merge fields into an existing value. Reads the current value, merges the updates, "
     "and writes it back atomically in a single transaction. If the key does not exist, "
     "creates a new entry with the given fields. Returns the merged table."},
    {"dissoc", jbolt_dissoc,
     "(jbolt/dissoc db bucket key & keys-to-remove)\n\n"
     "Remove fields from a stored value. Reads the current value, removes the specified "
     "keys, and writes it back atomically. Returns the updated table, or nil if the key "
     "does not exist."},
    {"each", jbolt_each,
     "(jbolt/each db bucket f)\n\n"
     "Iterate over all entries in a bucket in key order. "
     "Calls (f key value) for each entry."},
    {"collect", jbolt_collect,
     "(jbolt/collect db bucket)\n\n"
     "Return all entries in a bucket as an array of [key value] tuples, in key order."},
    {"map", jbolt_map,
     "(jbolt/map db bucket f)\n\n"
     "Map over all entries in a bucket in key order. "
     "Returns an array of (f key value) results."},
    {"filter", jbolt_filter,
     "(jbolt/filter db bucket f)\n\n"
     "Filter entries in a bucket. Returns an array of [key value] tuples "
     "for which (f key value) is truthy."},
    {"keys", jbolt_keys,
     "(jbolt/keys db bucket)\n\n"
     "Return all keys in a bucket as an array, in sorted order. "
     "Does not deserialize values."},
    {"count", jbolt_count,
     "(jbolt/count db bucket)\n\n"
     "Return the number of entries in a bucket. Returns 0 if the bucket does not exist."},
    {"first", jbolt_first,
     "(jbolt/first db bucket)\n\n"
     "Return the first entry [key value] in a bucket (by key order), or nil if empty."},
    {"last", jbolt_last,
     "(jbolt/last db bucket)\n\n"
     "Return the last entry [key value] in a bucket (by key order), or nil if empty."},
    {"seek", jbolt_seek,
     "(jbolt/seek db bucket key)\n\n"
     "Find the first entry with a key >= the given key. "
     "Returns [key value] or nil if no such key exists."},
    {"prefix", jbolt_prefix,
     "(jbolt/prefix db bucket prefix f)\n\n"
     "Iterate over all entries whose key starts with `prefix`, in key order. "
     "Calls (f key value) for each match."},
    {"range", jbolt_range,
     "(jbolt/range db bucket start end f)\n\n"
     "Iterate over all entries with keys between `start` and `end` (inclusive), "
     "in key order. Calls (f key value) for each entry."},
    {"update", jbolt_update,
     "(jbolt/update db f)\n\n"
     "Execute a read-write transaction. Calls (f tx) with a transaction handle. "
     "Commits on success, rolls back if f raises an error. Returns the result of f."},
    {"view", jbolt_view,
     "(jbolt/view db f)\n\n"
     "Execute a read-only transaction. Calls (f tx) with a transaction handle. "
     "Returns the result of f."},
    {"tx-put", jbolt_tx_put,
     "(jbolt/tx-put tx bucket key value)\n\n"
     "Store a value within an explicit transaction. Use inside jbolt/update."},
    {"tx-get", jbolt_tx_get,
     "(jbolt/tx-get tx bucket key)\n\n"
     "Retrieve a value within an explicit transaction. Returns nil if not found. "
     "Use inside jbolt/update or jbolt/view."},
    {"tx-delete", jbolt_tx_delete,
     "(jbolt/tx-delete tx bucket key)\n\n"
     "Delete a key within an explicit transaction. Use inside jbolt/update."},
    {"next-id", jbolt_next_id,
     "(jbolt/next-id db bucket)\n\n"
     "Return the next auto-increment ID for a bucket. "
     "Starts at 1, increments atomically on each call. "
     "Uses a reserved key \"__seq__\" internally."},
    {"backup", jbolt_backup,
     "(jbolt/backup db path)\n\n"
     "Write a consistent, compacted snapshot of the database to `path`. "
     "Safe to call while the database is open and in use."},
    {"stats", jbolt_stats,
     "(jbolt/stats db bucket)\n\n"
     "Return statistics for a bucket as a struct with keys:\n"
     "  :entries  :depth  :page-size  :branch-pages  :leaf-pages  :overflow-pages"},
    {"db-stats", jbolt_db_stats,
     "(jbolt/db-stats db)\n\n"
     "Return database-level statistics as a struct with keys:\n"
     "  :entries  :depth  :page-size  :map-size  :last-page  :last-txn\n"
     "  :max-readers  :num-readers"},
    {NULL, NULL, NULL}
};

JANET_MODULE_ENTRY(JanetTable *env) {
    janet_cfuns(env, "jbolt", cfuns);
}
