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

#define JBOLT_META_BUCKET  "__jbolt_meta__"
#define JBOLT_META_PREFIX  "__jbolt_"

/* ----------------------------------------------------------------
 * Error handling
 * ---------------------------------------------------------------- */

static void jbolt_panic_rc(int rc) {
    if (rc == MDB_MAP_FULL) {
        janet_panic("jbolt: database full (MDB_MAP_FULL) — increase :map-size");
    }
    if (rc == MDB_DBS_FULL) {
        janet_panic("jbolt: too many buckets (MDB_DBS_FULL) — increase :max-buckets");
    }
    janet_panicf("jbolt: %s", mdb_strerror(rc));
}

#define JBOLT_CHECK(rc) do { \
    int _rc = (rc); \
    if (_rc != MDB_SUCCESS) { \
        jbolt_panic_rc(_rc); \
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

static const JanetAbstractType jbolt_db_type = {
    "jbolt.db",
    jbolt_db_gc,
    NULL,
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
 * DBI helpers
 *
 * NOTE: We intentionally do NOT cache DBI handles. LMDB auto-closes any
 * handle that was opened in an aborted transaction (mdb_dbi_open docs:
 * "If the transaction is aborted the handle will be closed automatically").
 * Since our read-only operations always abort their txn, a cached handle
 * can become stale and later cursor ops fail with EINVAL.
 * mdb_dbi_open itself is idempotent and cheap, so we just always call it.
 *
 * Two variants for the two txn-ownership models:
 *   jbolt_open_dbi — for tx-* functions. Txn belongs to user code; on error
 *                    we only panic (update/view wrapper aborts the txn).
 *   jbolt_dbi_for  — for functions that own a local txn. Aborts the txn
 *                    before panicking. Signals bucket-not-found via -1.
 * ---------------------------------------------------------------- */

static MDB_dbi jbolt_open_dbi(JBoltDB *db, MDB_txn *txn,
                               const char *bucket, unsigned int flags) {
    (void)db;
    MDB_dbi dbi;
    int rc = mdb_dbi_open(txn, bucket, flags, &dbi);
    if (rc == MDB_NOTFOUND) {
        janet_panicf("jbolt: bucket not found: %s", bucket);
    }
    JBOLT_CHECK(rc);
    return dbi;
}

static int jbolt_dbi_for(JBoltDB *db, MDB_txn *txn, const char *bucket,
                         unsigned int flags, MDB_dbi *dbi_out) {
    (void)db;
    int rc = mdb_dbi_open(txn, bucket, flags, dbi_out);
    if (rc == MDB_NOTFOUND) return -1;
    if (rc != MDB_SUCCESS) {
        mdb_txn_abort(txn);
        jbolt_panic_rc(rc);
    }
    return 0;
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
        jbolt_panic_rc(rc);
    }

    JBoltDB *db = janet_abstract(&jbolt_db_type, sizeof(JBoltDB));
    db->env = env;
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
    (void)jbolt_dbi_for(db, txn, name, MDB_CREATE, &dbi);
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
    if (jbolt_dbi_for(db, txn, name, 0, &dbi) < 0) {
        mdb_txn_abort(txn);
        janet_panicf("jbolt: bucket not found: %s", name);
    }
    int rc = mdb_drop(txn, dbi, 1);
    if (rc != MDB_SUCCESS) {
        mdb_txn_abort(txn);
        jbolt_panic_rc(rc);
    }

    /* Also drop the sequence entry in the meta bucket (if any), so a recreated
     * bucket of the same name restarts from 1. Missing meta bucket is fine. */
    MDB_dbi meta_dbi;
    if (jbolt_dbi_for(db, txn, JBOLT_META_BUCKET, 0, &meta_dbi) == 0) {
        MDB_val mkey = {strlen(name), (void *)name};
        int mrc = mdb_del(txn, meta_dbi, &mkey, NULL);
        if (mrc != MDB_SUCCESS && mrc != MDB_NOTFOUND) {
            mdb_txn_abort(txn);
            jbolt_panic_rc(mrc);
        }
    }

    JBOLT_CHECK(mdb_txn_commit(txn));
    return janet_wrap_nil();
}

static Janet jbolt_has_bucket(int32_t argc, Janet *argv) {
    janet_fixarity(argc, 2);
    JBoltDB *db = janet_getabstract(argv, 0, &jbolt_db_type);
    jbolt_check_open(db->flags);
    const char *name = (const char *)janet_getstring(argv, 1);

    /* Reserved internal buckets are hidden from the public API,
     * matching the behavior of jbolt_buckets. */
    if (strncmp(name, JBOLT_META_PREFIX, strlen(JBOLT_META_PREFIX)) == 0) {
        return janet_wrap_boolean(0);
    }

    MDB_txn *txn;
    JBOLT_CHECK(mdb_txn_begin(db->env, NULL, MDB_RDONLY, &txn));

    MDB_dbi dbi;
    int rc = mdb_dbi_open(txn, name, 0, &dbi);
    /* The dbi is auto-released on abort, so no leak even on success. */
    mdb_txn_abort(txn);

    if (rc == MDB_SUCCESS) return janet_wrap_boolean(1);
    if (rc == MDB_NOTFOUND) return janet_wrap_boolean(0);
    jbolt_panic_rc(rc);
    return janet_wrap_nil(); /* unreachable */
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
        jbolt_panic_rc(rc);
    }

    MDB_cursor *cursor;
    rc = mdb_cursor_open(txn, dbi, &cursor);
    if (rc != MDB_SUCCESS) {
        mdb_txn_abort(txn);
        jbolt_panic_rc(rc);
    }

    JanetArray *arr = janet_array(8);
    MDB_val key, val;
    size_t meta_prefix_len = strlen(JBOLT_META_PREFIX);
    while ((rc = mdb_cursor_get(cursor, &key, &val, MDB_NEXT)) == MDB_SUCCESS) {
        /* Hide reserved internal buckets (JBOLT_META_PREFIX "__jbolt_"). */
        if (key.mv_size >= meta_prefix_len &&
            memcmp(key.mv_data, JBOLT_META_PREFIX, meta_prefix_len) == 0) {
            continue;
        }
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
    (void)jbolt_dbi_for(db, txn, bucket, MDB_CREATE, &dbi);

    MDB_val mkey = {strlen(keystr), (void *)keystr};
    MDB_val mval = {buf->count, buf->data};

    int rc = mdb_put(txn, dbi, &mkey, &mval, 0);
    if (rc != MDB_SUCCESS) {
        mdb_txn_abort(txn);
        jbolt_panic_rc(rc);
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
    if (jbolt_dbi_for(db, txn, bucket, 0, &dbi) < 0) {
        mdb_txn_abort(txn);
        return janet_wrap_nil();
    }

    MDB_val mkey = {strlen(keystr), (void *)keystr};
    MDB_val mval;

    int rc = mdb_get(txn, dbi, &mkey, &mval);
    if (rc == MDB_NOTFOUND) {
        mdb_txn_abort(txn);
        return janet_wrap_nil();
    }
    if (rc != MDB_SUCCESS) {
        mdb_txn_abort(txn);
        jbolt_panic_rc(rc);
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
    if (jbolt_dbi_for(db, txn, bucket, 0, &dbi) < 0) {
        mdb_txn_abort(txn);
        janet_panicf("jbolt: bucket not found: %s", bucket);
    }

    MDB_val mkey = {strlen(keystr), (void *)keystr};
    int rc = mdb_del(txn, dbi, &mkey, NULL);
    if (rc != MDB_SUCCESS && rc != MDB_NOTFOUND) {
        mdb_txn_abort(txn);
        jbolt_panic_rc(rc);
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
    if (jbolt_dbi_for(db, txn, bucket, 0, &dbi) < 0) {
        mdb_txn_abort(txn);
        return janet_wrap_boolean(0);
    }

    MDB_val mkey = {strlen(keystr), (void *)keystr};
    MDB_val mval;
    int rc = mdb_get(txn, dbi, &mkey, &mval);
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
    (void)jbolt_dbi_for(db, txn, bucket, MDB_CREATE, &dbi);

    /* Read existing value */
    MDB_val mkey = {strlen(keystr), (void *)keystr};
    MDB_val mval;
    JanetTable *tbl;

    int rc = mdb_get(txn, dbi, &mkey, &mval);
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
        jbolt_panic_rc(rc);
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
        jbolt_panic_rc(rc);
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
    if (jbolt_dbi_for(db, txn, bucket, 0, &dbi) < 0) {
        mdb_txn_abort(txn);
        janet_panicf("jbolt: bucket not found: %s", bucket);
    }

    MDB_val mkey = {strlen(keystr), (void *)keystr};
    MDB_val mval;

    int rc = mdb_get(txn, dbi, &mkey, &mval);
    if (rc == MDB_NOTFOUND) {
        mdb_txn_abort(txn);
        return janet_wrap_nil();
    }
    if (rc != MDB_SUCCESS) {
        mdb_txn_abort(txn);
        jbolt_panic_rc(rc);
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
        jbolt_panic_rc(rc);
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
    if (jbolt_dbi_for(db, txn, bucket, 0, &dbi) < 0) {
        mdb_txn_abort(txn);
        *txn_out = NULL;
        return -1;
    }

    MDB_cursor *cursor;
    int rc = mdb_cursor_open(txn, dbi, &cursor);
    if (rc != MDB_SUCCESS) {
        mdb_txn_abort(txn);
        jbolt_panic_rc(rc);
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

/* Iteration break sentinel: callback returns :break to stop early.
 * Chosen over "truthy stops" so that common patterns like
 *   (fn [k v] (array/push results k))
 * keep working — array/push returns a truthy array. */
static int jbolt_is_break(Janet v) {
    return janet_checktype(v, JANET_KEYWORD) && janet_keyeq(v, "break");
}

/* Same pattern for tx-* iteration: txn is user-owned, so only the cursor is
 * cleaned up here — the enclosing update/view pcall aborts the txn. */
static int jbolt_tx_iter_begin(JBoltDB *db, MDB_txn *txn, const char *bucket,
                                MDB_cursor **cur_out) {
    (void)db;
    MDB_dbi dbi;
    int rc = mdb_dbi_open(txn, bucket, 0, &dbi);
    if (rc == MDB_NOTFOUND) return -1;
    JBOLT_CHECK(rc);
    JBOLT_CHECK(mdb_cursor_open(txn, dbi, cur_out));
    return 0;
}

static JanetSignal jbolt_tx_call_safe(JanetFunction *fn, int32_t argn,
                                       const Janet *argv, Janet *out,
                                       MDB_cursor *cursor) {
    JanetSignal sig = janet_pcall(fn, argn, argv, out, NULL);
    if (sig != JANET_SIGNAL_OK) {
        mdb_cursor_close(cursor);
        janet_panicv(*out);
    }
    return sig;
}

/* ----------------------------------------------------------------
 * Phase 2: each, collect, map, filter
 * ---------------------------------------------------------------- */

/* Parse &named :reverse from argv starting at `from`. */
static int jbolt_parse_reverse(int32_t argc, const Janet *argv, int32_t from) {
    int reverse = 0;
    for (int32_t i = from; i < argc; i += 2) {
        if (i + 1 >= argc) janet_panic("jbolt: expected value after keyword");
        if (janet_keyeq(argv[i], "reverse")) {
            reverse = janet_truthy(argv[i + 1]);
        } else {
            janet_panicf("jbolt: unknown option %v", argv[i]);
        }
    }
    return reverse;
}

static Janet jbolt_each(int32_t argc, Janet *argv) {
    janet_arity(argc, 3, -1);
    JBoltDB *db = janet_getabstract(argv, 0, &jbolt_db_type);
    jbolt_check_open(db->flags);
    const char *bucket = (const char *)janet_getstring(argv, 1);
    JanetFunction *fn = janet_getfunction(argv, 2);
    int reverse = jbolt_parse_reverse(argc, argv, 3);

    MDB_txn *txn; MDB_dbi dbi; MDB_cursor *cursor;
    if (jbolt_iter_begin(db, bucket, &txn, &dbi, &cursor) < 0) {
        return janet_wrap_nil();
    }

    MDB_cursor_op first_op = reverse ? MDB_LAST : MDB_FIRST;
    MDB_cursor_op next_op = reverse ? MDB_PREV : MDB_NEXT;
    MDB_val key, val;
    int rc = mdb_cursor_get(cursor, &key, &val, first_op);
    while (rc == MDB_SUCCESS) {
        Janet args[2];
        args[0] = janet_stringv(key.mv_data, key.mv_size);
        args[1] = jbolt_unmarshal(val.mv_data, val.mv_size);
        Janet out;
        jbolt_call_safe(fn, 2, args, &out, cursor, txn);
        if (jbolt_is_break(out)) break;
        rc = mdb_cursor_get(cursor, &key, &val, next_op);
    }

    jbolt_iter_end(cursor, txn);
    return janet_wrap_nil();
}

static Janet jbolt_collect(int32_t argc, Janet *argv) {
    janet_arity(argc, 2, -1);
    JBoltDB *db = janet_getabstract(argv, 0, &jbolt_db_type);
    jbolt_check_open(db->flags);
    const char *bucket = (const char *)janet_getstring(argv, 1);
    int reverse = jbolt_parse_reverse(argc, argv, 2);

    MDB_txn *txn; MDB_dbi dbi; MDB_cursor *cursor;
    if (jbolt_iter_begin(db, bucket, &txn, &dbi, &cursor) < 0) {
        return janet_wrap_array(janet_array(0));
    }

    MDB_cursor_op first_op = reverse ? MDB_LAST : MDB_FIRST;
    MDB_cursor_op next_op = reverse ? MDB_PREV : MDB_NEXT;
    JanetArray *arr = janet_array(16);
    MDB_val key, val;
    int rc = mdb_cursor_get(cursor, &key, &val, first_op);
    while (rc == MDB_SUCCESS) {
        Janet tuple[2];
        tuple[0] = janet_stringv(key.mv_data, key.mv_size);
        tuple[1] = jbolt_unmarshal(val.mv_data, val.mv_size);
        janet_array_push(arr, janet_wrap_tuple(janet_tuple_n(tuple, 2)));
        rc = mdb_cursor_get(cursor, &key, &val, next_op);
    }

    jbolt_iter_end(cursor, txn);
    return janet_wrap_array(arr);
}

static Janet jbolt_map(int32_t argc, Janet *argv) {
    janet_arity(argc, 3, -1);
    JBoltDB *db = janet_getabstract(argv, 0, &jbolt_db_type);
    jbolt_check_open(db->flags);
    const char *bucket = (const char *)janet_getstring(argv, 1);
    JanetFunction *fn = janet_getfunction(argv, 2);
    int reverse = jbolt_parse_reverse(argc, argv, 3);

    MDB_txn *txn; MDB_dbi dbi; MDB_cursor *cursor;
    if (jbolt_iter_begin(db, bucket, &txn, &dbi, &cursor) < 0) {
        return janet_wrap_array(janet_array(0));
    }

    MDB_cursor_op first_op = reverse ? MDB_LAST : MDB_FIRST;
    MDB_cursor_op next_op = reverse ? MDB_PREV : MDB_NEXT;
    JanetArray *arr = janet_array(16);
    MDB_val key, val;
    int rc = mdb_cursor_get(cursor, &key, &val, first_op);
    while (rc == MDB_SUCCESS) {
        Janet args[2];
        args[0] = janet_stringv(key.mv_data, key.mv_size);
        args[1] = jbolt_unmarshal(val.mv_data, val.mv_size);
        Janet out;
        jbolt_call_safe(fn, 2, args, &out, cursor, txn);
        janet_array_push(arr, out);
        rc = mdb_cursor_get(cursor, &key, &val, next_op);
    }

    jbolt_iter_end(cursor, txn);
    return janet_wrap_array(arr);
}

static Janet jbolt_filter(int32_t argc, Janet *argv) {
    janet_arity(argc, 3, -1);
    JBoltDB *db = janet_getabstract(argv, 0, &jbolt_db_type);
    jbolt_check_open(db->flags);
    const char *bucket = (const char *)janet_getstring(argv, 1);
    JanetFunction *fn = janet_getfunction(argv, 2);
    int reverse = jbolt_parse_reverse(argc, argv, 3);

    MDB_txn *txn; MDB_dbi dbi; MDB_cursor *cursor;
    if (jbolt_iter_begin(db, bucket, &txn, &dbi, &cursor) < 0) {
        return janet_wrap_array(janet_array(0));
    }

    MDB_cursor_op first_op = reverse ? MDB_LAST : MDB_FIRST;
    MDB_cursor_op next_op = reverse ? MDB_PREV : MDB_NEXT;
    JanetArray *arr = janet_array(16);
    MDB_val key, val;
    int rc = mdb_cursor_get(cursor, &key, &val, first_op);
    while (rc == MDB_SUCCESS) {
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
        rc = mdb_cursor_get(cursor, &key, &val, next_op);
    }

    jbolt_iter_end(cursor, txn);
    return janet_wrap_array(arr);
}

/* ----------------------------------------------------------------
 * Phase 2: keys, count, first, last, seek
 * ---------------------------------------------------------------- */

static Janet jbolt_keys(int32_t argc, Janet *argv) {
    janet_arity(argc, 2, -1);
    JBoltDB *db = janet_getabstract(argv, 0, &jbolt_db_type);
    jbolt_check_open(db->flags);
    const char *bucket = (const char *)janet_getstring(argv, 1);
    int reverse = jbolt_parse_reverse(argc, argv, 2);

    MDB_txn *txn; MDB_dbi dbi; MDB_cursor *cursor;
    if (jbolt_iter_begin(db, bucket, &txn, &dbi, &cursor) < 0) {
        return janet_wrap_array(janet_array(0));
    }

    MDB_cursor_op first_op = reverse ? MDB_LAST : MDB_FIRST;
    MDB_cursor_op next_op = reverse ? MDB_PREV : MDB_NEXT;
    JanetArray *arr = janet_array(16);
    MDB_val key, val;
    int rc = mdb_cursor_get(cursor, &key, &val, first_op);
    while (rc == MDB_SUCCESS) {
        janet_array_push(arr, janet_stringv(key.mv_data, key.mv_size));
        rc = mdb_cursor_get(cursor, &key, &val, next_op);
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
    if (jbolt_dbi_for(db, txn, bucket, 0, &dbi) < 0) {
        mdb_txn_abort(txn);
        return janet_wrap_integer(0);
    }

    MDB_stat stat;
    int rc = mdb_stat(txn, dbi, &stat);
    mdb_txn_abort(txn);
    if (rc != MDB_SUCCESS) {
        jbolt_panic_rc(rc);
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
        if (jbolt_is_break(out)) break;
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
        if (jbolt_is_break(out)) break;
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

    MDB_dbi dbi;
    int rc = mdb_dbi_open(tx->txn, bucket, 0, &dbi);
    if (rc == MDB_NOTFOUND) return janet_wrap_nil();
    JBOLT_CHECK(rc);

    MDB_val mkey = {strlen(keystr), (void *)keystr};
    MDB_val mval;
    rc = mdb_get(tx->txn, dbi, &mkey, &mval);
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
        jbolt_panic_rc(rc);
    }
    return janet_wrap_nil();
}

/* Helper: resolve bucket -> dbi for read-only tx-* ops. Returns -1 on NOTFOUND
 * (caller returns the empty answer), panics on other errors. */
static int jbolt_tx_dbi_ro(JBoltTx *tx, const char *bucket, MDB_dbi *dbi_out) {
    int rc = mdb_dbi_open(tx->txn, bucket, 0, dbi_out);
    if (rc == MDB_NOTFOUND) return -1;
    JBOLT_CHECK(rc);
    return 0;
}

static Janet jbolt_tx_has(int32_t argc, Janet *argv) {
    janet_fixarity(argc, 3);
    JBoltTx *tx = janet_getabstract(argv, 0, &jbolt_tx_type);
    jbolt_check_tx(tx);
    const char *bucket = (const char *)janet_getstring(argv, 1);
    const char *keystr = (const char *)janet_getstring(argv, 2);

    MDB_dbi dbi;
    if (jbolt_tx_dbi_ro(tx, bucket, &dbi) < 0) return janet_wrap_boolean(0);

    MDB_val mkey = {strlen(keystr), (void *)keystr};
    MDB_val mval;
    int rc = mdb_get(tx->txn, dbi, &mkey, &mval);
    return janet_wrap_boolean(rc == MDB_SUCCESS);
}

static Janet jbolt_tx_has_bucket(int32_t argc, Janet *argv) {
    janet_fixarity(argc, 2);
    JBoltTx *tx = janet_getabstract(argv, 0, &jbolt_tx_type);
    jbolt_check_tx(tx);
    const char *name = (const char *)janet_getstring(argv, 1);

    /* Reserved internal buckets are hidden from the public API. */
    if (strncmp(name, JBOLT_META_PREFIX, strlen(JBOLT_META_PREFIX)) == 0) {
        return janet_wrap_boolean(0);
    }

    MDB_dbi dbi;
    int rc = mdb_dbi_open(tx->txn, name, 0, &dbi);
    if (rc == MDB_SUCCESS) return janet_wrap_boolean(1);
    if (rc == MDB_NOTFOUND) return janet_wrap_boolean(0);
    JBOLT_CHECK(rc);
    return janet_wrap_nil(); /* unreachable */
}

static Janet jbolt_tx_count(int32_t argc, Janet *argv) {
    janet_fixarity(argc, 2);
    JBoltTx *tx = janet_getabstract(argv, 0, &jbolt_tx_type);
    jbolt_check_tx(tx);
    const char *bucket = (const char *)janet_getstring(argv, 1);

    MDB_dbi dbi;
    if (jbolt_tx_dbi_ro(tx, bucket, &dbi) < 0) return janet_wrap_integer(0);

    MDB_stat stat;
    JBOLT_CHECK(mdb_stat(tx->txn, dbi, &stat));
    return janet_wrap_number((double)stat.ms_entries);
}

static Janet jbolt_tx_keys(int32_t argc, Janet *argv) {
    janet_arity(argc, 2, -1);
    JBoltTx *tx = janet_getabstract(argv, 0, &jbolt_tx_type);
    jbolt_check_tx(tx);
    const char *bucket = (const char *)janet_getstring(argv, 1);
    int reverse = jbolt_parse_reverse(argc, argv, 2);

    MDB_cursor *cursor;
    if (jbolt_tx_iter_begin(tx->db, tx->txn, bucket, &cursor) < 0) {
        return janet_wrap_array(janet_array(0));
    }

    MDB_cursor_op first_op = reverse ? MDB_LAST : MDB_FIRST;
    MDB_cursor_op next_op = reverse ? MDB_PREV : MDB_NEXT;
    JanetArray *arr = janet_array(16);
    MDB_val key, val;
    int rc = mdb_cursor_get(cursor, &key, &val, first_op);
    while (rc == MDB_SUCCESS) {
        janet_array_push(arr, janet_stringv(key.mv_data, key.mv_size));
        rc = mdb_cursor_get(cursor, &key, &val, next_op);
    }
    mdb_cursor_close(cursor);
    return janet_wrap_array(arr);
}

static Janet jbolt_tx_collect(int32_t argc, Janet *argv) {
    janet_arity(argc, 2, -1);
    JBoltTx *tx = janet_getabstract(argv, 0, &jbolt_tx_type);
    jbolt_check_tx(tx);
    const char *bucket = (const char *)janet_getstring(argv, 1);
    int reverse = jbolt_parse_reverse(argc, argv, 2);

    MDB_cursor *cursor;
    if (jbolt_tx_iter_begin(tx->db, tx->txn, bucket, &cursor) < 0) {
        return janet_wrap_array(janet_array(0));
    }

    MDB_cursor_op first_op = reverse ? MDB_LAST : MDB_FIRST;
    MDB_cursor_op next_op = reverse ? MDB_PREV : MDB_NEXT;
    JanetArray *arr = janet_array(16);
    MDB_val key, val;
    int rc = mdb_cursor_get(cursor, &key, &val, first_op);
    while (rc == MDB_SUCCESS) {
        Janet tuple[2];
        tuple[0] = janet_stringv(key.mv_data, key.mv_size);
        tuple[1] = jbolt_unmarshal(val.mv_data, val.mv_size);
        janet_array_push(arr, janet_wrap_tuple(janet_tuple_n(tuple, 2)));
        rc = mdb_cursor_get(cursor, &key, &val, next_op);
    }
    mdb_cursor_close(cursor);
    return janet_wrap_array(arr);
}

static Janet jbolt_tx_first(int32_t argc, Janet *argv) {
    janet_fixarity(argc, 2);
    JBoltTx *tx = janet_getabstract(argv, 0, &jbolt_tx_type);
    jbolt_check_tx(tx);
    const char *bucket = (const char *)janet_getstring(argv, 1);

    MDB_cursor *cursor;
    if (jbolt_tx_iter_begin(tx->db, tx->txn, bucket, &cursor) < 0) return janet_wrap_nil();

    MDB_val key, val;
    int rc = mdb_cursor_get(cursor, &key, &val, MDB_FIRST);
    Janet result = janet_wrap_nil();
    if (rc == MDB_SUCCESS) {
        Janet tuple[2];
        tuple[0] = janet_stringv(key.mv_data, key.mv_size);
        tuple[1] = jbolt_unmarshal(val.mv_data, val.mv_size);
        result = janet_wrap_tuple(janet_tuple_n(tuple, 2));
    }
    mdb_cursor_close(cursor);
    return result;
}

static Janet jbolt_tx_last(int32_t argc, Janet *argv) {
    janet_fixarity(argc, 2);
    JBoltTx *tx = janet_getabstract(argv, 0, &jbolt_tx_type);
    jbolt_check_tx(tx);
    const char *bucket = (const char *)janet_getstring(argv, 1);

    MDB_cursor *cursor;
    if (jbolt_tx_iter_begin(tx->db, tx->txn, bucket, &cursor) < 0) return janet_wrap_nil();

    MDB_val key, val;
    int rc = mdb_cursor_get(cursor, &key, &val, MDB_LAST);
    Janet result = janet_wrap_nil();
    if (rc == MDB_SUCCESS) {
        Janet tuple[2];
        tuple[0] = janet_stringv(key.mv_data, key.mv_size);
        tuple[1] = jbolt_unmarshal(val.mv_data, val.mv_size);
        result = janet_wrap_tuple(janet_tuple_n(tuple, 2));
    }
    mdb_cursor_close(cursor);
    return result;
}

static Janet jbolt_tx_seek(int32_t argc, Janet *argv) {
    janet_fixarity(argc, 3);
    JBoltTx *tx = janet_getabstract(argv, 0, &jbolt_tx_type);
    jbolt_check_tx(tx);
    const char *bucket = (const char *)janet_getstring(argv, 1);
    const char *keystr = (const char *)janet_getstring(argv, 2);

    MDB_cursor *cursor;
    if (jbolt_tx_iter_begin(tx->db, tx->txn, bucket, &cursor) < 0) return janet_wrap_nil();

    MDB_val key = {strlen(keystr), (void *)keystr};
    MDB_val val;
    int rc = mdb_cursor_get(cursor, &key, &val, MDB_SET_RANGE);
    Janet result = janet_wrap_nil();
    if (rc == MDB_SUCCESS) {
        Janet tuple[2];
        tuple[0] = janet_stringv(key.mv_data, key.mv_size);
        tuple[1] = jbolt_unmarshal(val.mv_data, val.mv_size);
        result = janet_wrap_tuple(janet_tuple_n(tuple, 2));
    }
    mdb_cursor_close(cursor);
    return result;
}

static Janet jbolt_tx_each(int32_t argc, Janet *argv) {
    janet_arity(argc, 3, -1);
    JBoltTx *tx = janet_getabstract(argv, 0, &jbolt_tx_type);
    jbolt_check_tx(tx);
    const char *bucket = (const char *)janet_getstring(argv, 1);
    JanetFunction *fn = janet_getfunction(argv, 2);
    int reverse = jbolt_parse_reverse(argc, argv, 3);

    MDB_cursor *cursor;
    if (jbolt_tx_iter_begin(tx->db, tx->txn, bucket, &cursor) < 0) return janet_wrap_nil();

    MDB_cursor_op first_op = reverse ? MDB_LAST : MDB_FIRST;
    MDB_cursor_op next_op = reverse ? MDB_PREV : MDB_NEXT;
    MDB_val key, val;
    int rc = mdb_cursor_get(cursor, &key, &val, first_op);
    while (rc == MDB_SUCCESS) {
        Janet args[2];
        args[0] = janet_stringv(key.mv_data, key.mv_size);
        args[1] = jbolt_unmarshal(val.mv_data, val.mv_size);
        Janet out;
        jbolt_tx_call_safe(fn, 2, args, &out, cursor);
        if (jbolt_is_break(out)) break;
        rc = mdb_cursor_get(cursor, &key, &val, next_op);
    }
    mdb_cursor_close(cursor);
    return janet_wrap_nil();
}

static Janet jbolt_tx_map(int32_t argc, Janet *argv) {
    janet_arity(argc, 3, -1);
    JBoltTx *tx = janet_getabstract(argv, 0, &jbolt_tx_type);
    jbolt_check_tx(tx);
    const char *bucket = (const char *)janet_getstring(argv, 1);
    JanetFunction *fn = janet_getfunction(argv, 2);
    int reverse = jbolt_parse_reverse(argc, argv, 3);

    MDB_cursor *cursor;
    if (jbolt_tx_iter_begin(tx->db, tx->txn, bucket, &cursor) < 0) {
        return janet_wrap_array(janet_array(0));
    }

    MDB_cursor_op first_op = reverse ? MDB_LAST : MDB_FIRST;
    MDB_cursor_op next_op = reverse ? MDB_PREV : MDB_NEXT;
    JanetArray *arr = janet_array(16);
    MDB_val key, val;
    int rc = mdb_cursor_get(cursor, &key, &val, first_op);
    while (rc == MDB_SUCCESS) {
        Janet args[2];
        args[0] = janet_stringv(key.mv_data, key.mv_size);
        args[1] = jbolt_unmarshal(val.mv_data, val.mv_size);
        Janet out;
        jbolt_tx_call_safe(fn, 2, args, &out, cursor);
        janet_array_push(arr, out);
        rc = mdb_cursor_get(cursor, &key, &val, next_op);
    }
    mdb_cursor_close(cursor);
    return janet_wrap_array(arr);
}

static Janet jbolt_tx_filter(int32_t argc, Janet *argv) {
    janet_arity(argc, 3, -1);
    JBoltTx *tx = janet_getabstract(argv, 0, &jbolt_tx_type);
    jbolt_check_tx(tx);
    const char *bucket = (const char *)janet_getstring(argv, 1);
    JanetFunction *fn = janet_getfunction(argv, 2);
    int reverse = jbolt_parse_reverse(argc, argv, 3);

    MDB_cursor *cursor;
    if (jbolt_tx_iter_begin(tx->db, tx->txn, bucket, &cursor) < 0) {
        return janet_wrap_array(janet_array(0));
    }

    MDB_cursor_op first_op = reverse ? MDB_LAST : MDB_FIRST;
    MDB_cursor_op next_op = reverse ? MDB_PREV : MDB_NEXT;
    JanetArray *arr = janet_array(16);
    MDB_val key, val;
    int rc = mdb_cursor_get(cursor, &key, &val, first_op);
    while (rc == MDB_SUCCESS) {
        Janet args[2];
        args[0] = janet_stringv(key.mv_data, key.mv_size);
        args[1] = jbolt_unmarshal(val.mv_data, val.mv_size);
        Janet out;
        jbolt_tx_call_safe(fn, 2, args, &out, cursor);
        if (janet_truthy(out)) {
            Janet tuple[2];
            tuple[0] = args[0];
            tuple[1] = args[1];
            janet_array_push(arr, janet_wrap_tuple(janet_tuple_n(tuple, 2)));
        }
        rc = mdb_cursor_get(cursor, &key, &val, next_op);
    }
    mdb_cursor_close(cursor);
    return janet_wrap_array(arr);
}

static Janet jbolt_tx_prefix(int32_t argc, Janet *argv) {
    janet_fixarity(argc, 4);
    JBoltTx *tx = janet_getabstract(argv, 0, &jbolt_tx_type);
    jbolt_check_tx(tx);
    const char *bucket = (const char *)janet_getstring(argv, 1);
    const uint8_t *prefix = janet_getstring(argv, 2);
    int32_t prefix_len = janet_string_length(prefix);
    JanetFunction *fn = janet_getfunction(argv, 3);

    MDB_cursor *cursor;
    if (jbolt_tx_iter_begin(tx->db, tx->txn, bucket, &cursor) < 0) return janet_wrap_nil();

    MDB_val key = {prefix_len, (void *)prefix};
    MDB_val val;
    int rc = mdb_cursor_get(cursor, &key, &val, MDB_SET_RANGE);
    while (rc == MDB_SUCCESS) {
        if ((int32_t)key.mv_size < prefix_len ||
            memcmp(key.mv_data, prefix, prefix_len) != 0) {
            break;
        }
        Janet args[2];
        args[0] = janet_stringv(key.mv_data, key.mv_size);
        args[1] = jbolt_unmarshal(val.mv_data, val.mv_size);
        Janet out;
        jbolt_tx_call_safe(fn, 2, args, &out, cursor);
        if (jbolt_is_break(out)) break;
        rc = mdb_cursor_get(cursor, &key, &val, MDB_NEXT);
    }
    mdb_cursor_close(cursor);
    return janet_wrap_nil();
}

static Janet jbolt_tx_range(int32_t argc, Janet *argv) {
    janet_fixarity(argc, 5);
    JBoltTx *tx = janet_getabstract(argv, 0, &jbolt_tx_type);
    jbolt_check_tx(tx);
    const char *bucket = (const char *)janet_getstring(argv, 1);
    const uint8_t *start = janet_getstring(argv, 2);
    int32_t start_len = janet_string_length(start);
    const uint8_t *end = janet_getstring(argv, 3);
    int32_t end_len = janet_string_length(end);
    JanetFunction *fn = janet_getfunction(argv, 4);

    MDB_cursor *cursor;
    if (jbolt_tx_iter_begin(tx->db, tx->txn, bucket, &cursor) < 0) return janet_wrap_nil();

    MDB_val key = {start_len, (void *)start};
    MDB_val val;
    int rc = mdb_cursor_get(cursor, &key, &val, MDB_SET_RANGE);
    while (rc == MDB_SUCCESS) {
        size_t min_len = key.mv_size < (size_t)end_len ? key.mv_size : (size_t)end_len;
        int cmp = memcmp(key.mv_data, end, min_len);
        if (cmp > 0 || (cmp == 0 && key.mv_size > (size_t)end_len)) break;

        Janet args[2];
        args[0] = janet_stringv(key.mv_data, key.mv_size);
        args[1] = jbolt_unmarshal(val.mv_data, val.mv_size);
        Janet out;
        jbolt_tx_call_safe(fn, 2, args, &out, cursor);
        if (jbolt_is_break(out)) break;
        rc = mdb_cursor_get(cursor, &key, &val, MDB_NEXT);
    }
    mdb_cursor_close(cursor);
    return janet_wrap_nil();
}

static Janet jbolt_tx_next_id(int32_t argc, Janet *argv) {
    janet_fixarity(argc, 2);
    JBoltTx *tx = janet_getabstract(argv, 0, &jbolt_tx_type);
    jbolt_check_tx(tx);
    if (tx->flags & JBOLT_TX_RDONLY) {
        janet_panic("jbolt: tx-next-id requires a read-write transaction");
    }
    const char *bucket = (const char *)janet_getstring(argv, 1);

    MDB_dbi meta_dbi = jbolt_open_dbi(tx->db, tx->txn, JBOLT_META_BUCKET, MDB_CREATE);

    MDB_val seq_key = {strlen(bucket), (void *)bucket};
    MDB_val seq_val;
    int64_t next = 1;

    int rc = mdb_get(tx->txn, meta_dbi, &seq_key, &seq_val);
    if (rc == MDB_SUCCESS) {
        Janet current = jbolt_unmarshal(seq_val.mv_data, seq_val.mv_size);
        if (janet_checktype(current, JANET_NUMBER)) {
            next = (int64_t)janet_unwrap_number(current) + 1;
        }
    } else if (rc != MDB_NOTFOUND) {
        jbolt_panic_rc(rc);
    }

    JanetBuffer *buf = janet_buffer(16);
    jbolt_marshal(buf, janet_wrap_number((double)next));
    MDB_val new_val = {buf->count, buf->data};
    JBOLT_CHECK(mdb_put(tx->txn, meta_dbi, &seq_key, &new_val, 0));
    return janet_wrap_number((double)next);
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

    /* Sequences live in the reserved meta bucket, keyed by target bucket name.
     * This keeps them out of user-visible iteration over the target bucket. */
    MDB_dbi meta_dbi;
    (void)jbolt_dbi_for(db, txn, JBOLT_META_BUCKET, MDB_CREATE, &meta_dbi);

    MDB_val seq_key = {strlen(bucket), (void *)bucket};
    MDB_val seq_val;
    int64_t next = 1;

    int rc = mdb_get(txn, meta_dbi, &seq_key, &seq_val);
    if (rc == MDB_SUCCESS) {
        Janet current = jbolt_unmarshal(seq_val.mv_data, seq_val.mv_size);
        if (janet_checktype(current, JANET_NUMBER)) {
            next = (int64_t)janet_unwrap_number(current) + 1;
        }
    } else if (rc != MDB_NOTFOUND) {
        mdb_txn_abort(txn);
        jbolt_panic_rc(rc);
    }

    JanetBuffer *buf = janet_buffer(16);
    jbolt_marshal(buf, janet_wrap_number((double)next));
    MDB_val new_val = {buf->count, buf->data};
    rc = mdb_put(txn, meta_dbi, &seq_key, &new_val, 0);
    if (rc != MDB_SUCCESS) {
        mdb_txn_abort(txn);
        jbolt_panic_rc(rc);
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

/* Collect all entries of a bucket as a pair-array, using the given txn.
 * Empty array if bucket does not exist. */
static JanetArray *jbolt_collect_entries(JBoltDB *db, MDB_txn *txn, const char *bucket) {
    MDB_dbi dbi;
    if (jbolt_dbi_for(db, txn, bucket, 0, &dbi) < 0) {
        return janet_array(0);
    }
    MDB_cursor *cursor;
    JBOLT_CHECK(mdb_cursor_open(txn, dbi, &cursor));
    JanetArray *arr = janet_array(16);
    MDB_val key, val;
    while (mdb_cursor_get(cursor, &key, &val, MDB_NEXT) == MDB_SUCCESS) {
        Janet tuple[2];
        tuple[0] = janet_stringv(key.mv_data, key.mv_size);
        tuple[1] = jbolt_unmarshal(val.mv_data, val.mv_size);
        janet_array_push(arr, janet_wrap_tuple(janet_tuple_n(tuple, 2)));
    }
    mdb_cursor_close(cursor);
    return arr;
}

static Janet jbolt_export_bucket(int32_t argc, Janet *argv) {
    janet_fixarity(argc, 2);
    JBoltDB *db = janet_getabstract(argv, 0, &jbolt_db_type);
    jbolt_check_open(db->flags);
    const char *bucket = (const char *)janet_getstring(argv, 1);

    MDB_txn *txn;
    JBOLT_CHECK(mdb_txn_begin(db->env, NULL, MDB_RDONLY, &txn));
    JanetArray *arr = jbolt_collect_entries(db, txn, bucket);
    mdb_txn_abort(txn);
    return janet_wrap_array(arr);
}

/* Write entries (indexed of [k v] pairs) into bucket via an open write txn.
 * On malformed input, aborts the txn and panics. */
static void jbolt_write_entries(JBoltDB *db, MDB_txn *txn, const char *bucket,
                                 Janet entries_val) {
    const Janet *entries;
    int32_t n_entries;
    if (!janet_indexed_view(entries_val, &entries, &n_entries)) {
        mdb_txn_abort(txn);
        janet_panic("jbolt: entries must be an array/tuple of [key value] pairs");
    }
    MDB_dbi dbi;
    (void)jbolt_dbi_for(db, txn, bucket, MDB_CREATE, &dbi);

    for (int32_t i = 0; i < n_entries; i++) {
        const Janet *pair;
        int32_t pair_len;
        if (!janet_indexed_view(entries[i], &pair, &pair_len) || pair_len != 2) {
            mdb_txn_abort(txn);
            janet_panic("jbolt: each entry must be a 2-element [key value] pair");
        }
        if (!janet_checktype(pair[0], JANET_STRING)) {
            mdb_txn_abort(txn);
            janet_panic("jbolt: entry key must be a string");
        }
        JanetString keystr = janet_unwrap_string(pair[0]);
        int32_t keylen = janet_string_length(keystr);

        JanetBuffer *buf = janet_buffer(64);
        jbolt_marshal(buf, pair[1]);

        MDB_val mkey = {(size_t)keylen, (void *)keystr};
        MDB_val mval = {buf->count, buf->data};
        int rc = mdb_put(txn, dbi, &mkey, &mval, 0);
        if (rc != MDB_SUCCESS) {
            mdb_txn_abort(txn);
            jbolt_panic_rc(rc);
        }
    }
}

static Janet jbolt_import_bucket(int32_t argc, Janet *argv) {
    janet_fixarity(argc, 3);
    JBoltDB *db = janet_getabstract(argv, 0, &jbolt_db_type);
    jbolt_check_open(db->flags);
    const char *bucket = (const char *)janet_getstring(argv, 1);
    Janet entries_val = argv[2];

    MDB_txn *txn;
    JBOLT_CHECK(mdb_txn_begin(db->env, NULL, 0, &txn));
    jbolt_write_entries(db, txn, bucket, entries_val);
    JBOLT_CHECK(mdb_txn_commit(txn));
    return janet_wrap_nil();
}

static Janet jbolt_import_db(int32_t argc, Janet *argv) {
    janet_fixarity(argc, 2);
    JBoltDB *db = janet_getabstract(argv, 0, &jbolt_db_type);
    jbolt_check_open(db->flags);
    Janet data = argv[1];

    /* Accept either a table (from in-memory construction or json/decode) or
     * a struct (from literal Janet source). */
    const JanetKV *kvs;
    int32_t cap;
    if (janet_checktype(data, JANET_TABLE)) {
        JanetTable *tbl = janet_unwrap_table(data);
        kvs = tbl->data;
        cap = tbl->capacity;
    } else if (janet_checktype(data, JANET_STRUCT)) {
        JanetStruct st = janet_unwrap_struct(data);
        kvs = st;
        cap = janet_struct_capacity(st);
    } else {
        janet_panic("jbolt: import-db expects a table/struct {bucket-name -> entries}");
    }

    MDB_txn *txn;
    JBOLT_CHECK(mdb_txn_begin(db->env, NULL, 0, &txn));

    const JanetKV *kv = NULL;
    while ((kv = janet_dictionary_next(kvs, cap, kv))) {
        /* Accept both strings and keywords as bucket names — after
         * keywordize-keys the top-level keys come back as keywords. Both share
         * the JanetString byte layout internally. */
        const uint8_t *bname;
        int32_t bname_len;
        if (janet_checktype(kv->key, JANET_STRING)) {
            JanetString s = janet_unwrap_string(kv->key);
            bname = s;
            bname_len = janet_string_length(s);
        } else if (janet_checktype(kv->key, JANET_KEYWORD)) {
            JanetKeyword k = janet_unwrap_keyword(kv->key);
            bname = k;
            bname_len = janet_string_length(k);
        } else {
            mdb_txn_abort(txn);
            janet_panic("jbolt: bucket names must be strings or keywords");
        }

        char buf[512];
        if (bname_len >= (int32_t)sizeof(buf)) {
            mdb_txn_abort(txn);
            janet_panic("jbolt: bucket name too long");
        }
        memcpy(buf, bname, bname_len);
        buf[bname_len] = '\0';

        jbolt_write_entries(db, txn, buf, kv->value);
    }

    JBOLT_CHECK(mdb_txn_commit(txn));
    return janet_wrap_nil();
}

static Janet jbolt_export_db(int32_t argc, Janet *argv) {
    janet_arity(argc, 1, -1);
    JBoltDB *db = janet_getabstract(argv, 0, &jbolt_db_type);
    jbolt_check_open(db->flags);

    int include_meta = 0;
    for (int32_t i = 1; i < argc; i += 2) {
        if (i + 1 >= argc) janet_panic("jbolt: expected value after keyword");
        if (janet_keyeq(argv[i], "include-meta")) {
            include_meta = janet_truthy(argv[i + 1]);
        } else {
            janet_panicf("jbolt: unknown option %v", argv[i]);
        }
    }

    MDB_txn *txn;
    JBOLT_CHECK(mdb_txn_begin(db->env, NULL, MDB_RDONLY, &txn));

    /* Phase 1: enumerate bucket names via the main DB cursor. Close that
     * cursor before starting per-bucket collection so cleanup stays simple. */
    MDB_dbi main_dbi;
    int rc = mdb_dbi_open(txn, NULL, 0, &main_dbi);
    if (rc != MDB_SUCCESS) {
        mdb_txn_abort(txn);
        jbolt_panic_rc(rc);
    }
    MDB_cursor *main_cursor;
    rc = mdb_cursor_open(txn, main_dbi, &main_cursor);
    if (rc != MDB_SUCCESS) {
        mdb_txn_abort(txn);
        jbolt_panic_rc(rc);
    }

    JanetArray *names = janet_array(8);
    size_t meta_prefix_len = strlen(JBOLT_META_PREFIX);
    MDB_val bkey, bval;
    while (mdb_cursor_get(main_cursor, &bkey, &bval, MDB_NEXT) == MDB_SUCCESS) {
        if (!include_meta &&
            bkey.mv_size >= meta_prefix_len &&
            memcmp(bkey.mv_data, JBOLT_META_PREFIX, meta_prefix_len) == 0) {
            continue;
        }
        janet_array_push(names, janet_stringv(bkey.mv_data, bkey.mv_size));
    }
    mdb_cursor_close(main_cursor);

    /* Phase 2: for each bucket, collect entries in the same snapshot txn. */
    JanetTable *result = janet_table(names->count);
    for (int32_t i = 0; i < names->count; i++) {
        JanetString name = janet_unwrap_string(names->data[i]);
        int32_t name_len = janet_string_length(name);

        /* mdb_dbi_open needs a NUL-terminated C string. */
        char buf[512];
        if (name_len >= (int32_t)sizeof(buf)) {
            mdb_txn_abort(txn);
            janet_panic("jbolt: bucket name too long");
        }
        memcpy(buf, name, name_len);
        buf[name_len] = '\0';

        JanetArray *entries = jbolt_collect_entries(db, txn, buf);
        janet_table_put(result, names->data[i], janet_wrap_array(entries));
    }
    mdb_txn_abort(txn);
    return janet_wrap_table(result);
}

/* Recursively replace string-typed dictionary keys with keywords. Preserves the
 * container kind (table→table, struct→struct, array→array, tuple→tuple) and
 * walks into nested containers. Non-key strings (i.e. values) are left alone;
 * a lossless Janet roundtrip would need JDN, not JSON + heuristics. */
static Janet jbolt_kwk_recurse(Janet v) {
    if (janet_checktype(v, JANET_TABLE)) {
        JanetTable *src = janet_unwrap_table(v);
        JanetTable *dst = janet_table(src->count);
        for (int32_t i = 0; i < src->capacity; i++) {
            JanetKV *kv = &src->data[i];
            if (janet_checktype(kv->key, JANET_NIL)) continue;
            Janet new_key = kv->key;
            if (janet_checktype(kv->key, JANET_STRING)) {
                JanetString s = janet_unwrap_string(kv->key);
                new_key = janet_keywordv(s, janet_string_length(s));
            }
            janet_table_put(dst, new_key, jbolt_kwk_recurse(kv->value));
        }
        return janet_wrap_table(dst);
    }
    if (janet_checktype(v, JANET_STRUCT)) {
        JanetStruct src = janet_unwrap_struct(v);
        int32_t count = janet_struct_length(src);
        int32_t cap = janet_struct_capacity(src);
        JanetKV *dst = janet_struct_begin(count);
        const JanetKV *kv = NULL;
        while ((kv = janet_dictionary_next(src, cap, kv))) {
            Janet new_key = kv->key;
            if (janet_checktype(kv->key, JANET_STRING)) {
                JanetString s = janet_unwrap_string(kv->key);
                new_key = janet_keywordv(s, janet_string_length(s));
            }
            janet_struct_put(dst, new_key, jbolt_kwk_recurse(kv->value));
        }
        return janet_wrap_struct(janet_struct_end(dst));
    }
    if (janet_checktype(v, JANET_ARRAY)) {
        JanetArray *src = janet_unwrap_array(v);
        JanetArray *dst = janet_array(src->count);
        for (int32_t i = 0; i < src->count; i++) {
            janet_array_push(dst, jbolt_kwk_recurse(src->data[i]));
        }
        return janet_wrap_array(dst);
    }
    if (janet_checktype(v, JANET_TUPLE)) {
        const Janet *src = janet_unwrap_tuple(v);
        int32_t len = janet_tuple_length(src);
        Janet *dst = janet_tuple_begin(len);
        for (int32_t i = 0; i < len; i++) {
            dst[i] = jbolt_kwk_recurse(src[i]);
        }
        return janet_wrap_tuple(janet_tuple_end(dst));
    }
    return v;
}

static Janet jbolt_keywordize_keys(int32_t argc, Janet *argv) {
    janet_fixarity(argc, 1);
    return jbolt_kwk_recurse(argv[0]);
}

static Janet jbolt_stats(int32_t argc, Janet *argv) {
    janet_fixarity(argc, 2);
    JBoltDB *db = janet_getabstract(argv, 0, &jbolt_db_type);
    jbolt_check_open(db->flags);
    const char *bucket = (const char *)janet_getstring(argv, 1);

    MDB_txn *txn;
    JBOLT_CHECK(mdb_txn_begin(db->env, NULL, MDB_RDONLY, &txn));

    MDB_dbi dbi;
    if (jbolt_dbi_for(db, txn, bucket, 0, &dbi) < 0) {
        mdb_txn_abort(txn);
        janet_panicf("jbolt: bucket not found: %s", bucket);
    }

    MDB_stat stat;
    int rc = mdb_stat(txn, dbi, &stat);
    mdb_txn_abort(txn);
    if (rc != MDB_SUCCESS) {
        jbolt_panic_rc(rc);
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
    {"has-bucket?", jbolt_has_bucket,
     "(jbolt/has-bucket? db name)\n\n"
     "Check if a bucket exists. Returns true or false. Reserved internal "
     "buckets (those with a `__jbolt_` prefix) are reported as not present."},
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
    {"tx-has?", jbolt_tx_has,
     "(jbolt/tx-has? tx bucket key)\n\n"
     "Check if a key exists in a bucket within an explicit transaction."},
    {"tx-has-bucket?", jbolt_tx_has_bucket,
     "(jbolt/tx-has-bucket? tx name)\n\n"
     "Check if a bucket exists within an explicit transaction. Reserved "
     "internal buckets (those with a `__jbolt_` prefix) are reported as not "
     "present."},
    {"tx-count", jbolt_tx_count,
     "(jbolt/tx-count tx bucket)\n\n"
     "Return the number of entries in a bucket within an explicit transaction."},
    {"tx-keys", jbolt_tx_keys,
     "(jbolt/tx-keys tx bucket)\n\n"
     "Return all keys in a bucket within an explicit transaction, in sorted order."},
    {"tx-collect", jbolt_tx_collect,
     "(jbolt/tx-collect tx bucket)\n\n"
     "Return all entries in a bucket as [key value] tuples within an explicit transaction."},
    {"tx-each", jbolt_tx_each,
     "(jbolt/tx-each tx bucket f)\n\n"
     "Iterate over all entries in a bucket within an explicit transaction. "
     "Calls (f key value) for each entry."},
    {"tx-map", jbolt_tx_map,
     "(jbolt/tx-map tx bucket f)\n\n"
     "Map over all entries within an explicit transaction. "
     "Returns an array of (f key value) results."},
    {"tx-filter", jbolt_tx_filter,
     "(jbolt/tx-filter tx bucket f)\n\n"
     "Filter entries within an explicit transaction. "
     "Returns an array of [key value] tuples for which (f key value) is truthy."},
    {"tx-first", jbolt_tx_first,
     "(jbolt/tx-first tx bucket)\n\n"
     "Return the first entry [key value] in a bucket within a transaction, or nil."},
    {"tx-last", jbolt_tx_last,
     "(jbolt/tx-last tx bucket)\n\n"
     "Return the last entry [key value] in a bucket within a transaction, or nil."},
    {"tx-seek", jbolt_tx_seek,
     "(jbolt/tx-seek tx bucket key)\n\n"
     "Find the first entry with a key >= the given key within a transaction, "
     "or nil. Use inside jbolt/update or jbolt/view."},
    {"tx-prefix", jbolt_tx_prefix,
     "(jbolt/tx-prefix tx bucket prefix f)\n\n"
     "Iterate over entries whose key starts with `prefix`, within a transaction."},
    {"tx-range", jbolt_tx_range,
     "(jbolt/tx-range tx bucket start end f)\n\n"
     "Iterate over entries with keys between `start` and `end` (inclusive), "
     "within a transaction."},
    {"tx-next-id", jbolt_tx_next_id,
     "(jbolt/tx-next-id tx bucket)\n\n"
     "Return the next auto-increment ID within an explicit read-write transaction. "
     "Enables atomic next-id-plus-put patterns."},
    {"next-id", jbolt_next_id,
     "(jbolt/next-id db bucket)\n\n"
     "Return the next auto-increment ID for a bucket. "
     "Starts at 1, increments atomically on each call."},
    {"backup", jbolt_backup,
     "(jbolt/backup db path)\n\n"
     "Write a consistent, compacted snapshot of the database to `path`. "
     "Safe to call while the database is open and in use."},
    {"export-bucket", jbolt_export_bucket,
     "(jbolt/export-bucket db bucket)\n\n"
     "Return all entries of a bucket as @[[key value] ...] (a pair array in key order). "
     "Intended for serialization — encode with json/encode or similar. "
     "Note: JSON is lossy (keywords become strings, tuples become arrays)."},
    {"export-db", jbolt_export_db,
     "(jbolt/export-db db &named :include-meta)\n\n"
     "Return the whole database as @{bucket-name @[[key value] ...]}, captured in a "
     "single read-only transaction (consistent snapshot). "
     "By default the internal meta bucket (next-id state) is omitted; pass "
     ":include-meta true to include it."},
    {"import-bucket", jbolt_import_bucket,
     "(jbolt/import-bucket db bucket entries)\n\n"
     "Write an array/tuple of [key value] pairs into bucket. Creates the bucket "
     "if it does not exist. Existing keys are overwritten (use drop-bucket first "
     "for a clean import). Inner pairs may be tuples or arrays."},
    {"import-db", jbolt_import_db,
     "(jbolt/import-db db data)\n\n"
     "Restore a database from a {bucket-name entries} table/struct. Entire import "
     "runs in a single write transaction — either everything commits or nothing does. "
     "Existing keys are overwritten; other buckets are left untouched."},
    {"keywordize-keys", jbolt_keywordize_keys,
     "(jbolt/keywordize-keys data)\n\n"
     "Recursively walk a Janet value and convert all string-typed dictionary keys "
     "into keywords. Preserves container kind (table→table, struct→struct, "
     "array→array, tuple→tuple). Intended as a post-processing step after "
     "json/decode so that {\"name\" \"Axel\"} becomes {:name \"Axel\"} before "
     "import. Values are left untouched — a full Janet roundtrip needs JDN."},
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
