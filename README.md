# jbolt

**jbolt** is an embedded, schema-free key-value store for [Janet](https://janet-lang.org), built on top of [LMDB](https://www.symas.com/lmdb) (Lightning Memory-Mapped Database).

Spiritually inspired by [BoltDB](https://github.com/boltdb/bolt) and [bbolt](https://github.com/etcd-io/bbolt) — which was originally a Go port of LMDB. jbolt brings the same simple, bucket-oriented philosophy to Janet, using LMDB as the storage engine and Janet's native `marshal`/`unmarshal` for serialization.

## Why jbolt?

- **No schema.** Store any Janet value — tables, structs, arrays, tuples, strings, numbers — directly. No migrations needed.
- **No server.** jbolt is an embedded library. Your database is your process.
- **Single file.** The entire database lives in one file. Backup = `cp`.
- **ACID transactions.** Fully serializable, crash-safe writes via LMDB's copy-on-write B+tree.
- **Buckets.** Data is organized into named buckets, equivalent to bbolt's `Bucket` concept.
- **Fast reads.** LMDB uses memory-mapped files — reads are zero-copy and extremely fast.

## Installation

```sh
jpm install https://github.com/axelrhd-janet/jbolt.git
```

Or add it as a dependency in your `project.janet`:

```janet
(declare-project
  :name "my-app"
  :dependencies ["https://github.com/axelrhd-janet/jbolt.git"])
```

No system dependencies required. LMDB is vendored and compiled together with the native module. A C compiler is all you need.

## Quick start

```janet
(import jbolt)

# Open or create a database
(def db (jbolt/open "myapp.mdb"))

# Create a bucket
(jbolt/ensure-bucket db "users")

# Store a value — any Janet value works
(jbolt/put db "users" "usr-001" {:name "Axel"
                                  :email "axel@example.com"
                                  :role :admin})

# Read it back
(jbolt/get db "users" "usr-001")
# => {:name "Axel" :email "axel@example.com" :role :admin}

# Update individual fields atomically
(jbolt/merge db "users" "usr-001" {:email "new@example.com"})

# Iterate over all entries
(jbolt/each db "users"
  (fn [k v] (print k " -> " (v :name))))
```

## API

### Database lifecycle

```janet
# Open with optional parameters
(def db (jbolt/open "myapp.mdb" :max-buckets 32 :map-size (* 512 1024 1024) :mode 0664))

# Close (safe to call multiple times)
(jbolt/close db)
```

### Bucket management

```janet
(jbolt/ensure-bucket db "users")     # create if not exists
(jbolt/drop-bucket db "sessions")    # delete bucket and all contents
(jbolt/buckets db)                   # => @["users" "sessions"]
```

### CRUD

```janet
(jbolt/put db "users" "usr-001" {:name "Axel" :role :admin})
(jbolt/get db "users" "usr-001")       # => {:name "Axel" :role :admin}
(jbolt/has? db "users" "usr-001")      # => true
(jbolt/delete db "users" "usr-001")
```

### Atomic field operations

```janet
# Merge fields into an existing value (read-modify-write in one transaction)
(jbolt/merge db "users" "usr-001" {:email "new@example.com" :verified true})

# Remove fields from an existing value
(jbolt/dissoc db "users" "usr-001" :verified :tmp)
```

### Iteration

All iteration functions operate in key-sorted order.

```janet
# Iterate with callback
(jbolt/each db "users" (fn [k v] (print k)))

# Collect all entries as [key value] tuples
(jbolt/collect db "users")
# => @[["usr-001" {:name "Axel" ...}] ...]

# Map over entries
(jbolt/map db "users" (fn [k v] (v :name)))
# => @["Axel" ...]

# Filter entries
(jbolt/filter db "users" (fn [k v] (= (v :role) :admin)))
# => @[["usr-001" {:name "Axel" ...}] ...]

# All keys (without deserializing values)
(jbolt/keys db "users")
# => @["usr-001" "usr-002" ...]

# Entry count
(jbolt/count db "users")
# => 42
```

### Cursor operations

```janet
(jbolt/first db "users")              # first entry [key value] or nil
(jbolt/last db "users")               # last entry [key value] or nil
(jbolt/seek db "users" "usr-01HX")    # first key >= "usr-01HX", or nil
```

### Range and prefix scans

```janet
# Iterate keys starting with a prefix
(jbolt/prefix db "users" "usr-01HX"
  (fn [k v] (print k)))

# Iterate keys between start and end (inclusive)
(jbolt/range db "users" "usr-001" "usr-099"
  (fn [k v] (print k)))
```

### Transactions

Individual `put`/`get`/`delete` calls use implicit transactions. For atomic multi-key operations, use explicit transactions:

```janet
# Read-write transaction — commits on success, rolls back on error
(jbolt/update db
  (fn [tx]
    (jbolt/tx-put tx "users" "usr-001" {:name "Axel"})
    (jbolt/tx-put tx "users" "usr-002" {:name "Ben"})
    (jbolt/tx-delete tx "users" "usr-old")))

# Read-only transaction
(jbolt/view db
  (fn [tx]
    (jbolt/tx-get tx "users" "usr-001")))
```

### Utility

```janet
# Auto-increment ID (starts at 1, atomic)
(jbolt/next-id db "users")    # => 1
(jbolt/next-id db "users")    # => 2

# Consistent backup (safe while database is in use)
(jbolt/backup db "/backups/myapp-2024-03-01.mdb")

# Bucket statistics
(jbolt/stats db "users")
# => {:entries 42 :depth 2 :page-size 4096 :branch-pages 1 :leaf-pages 3 :overflow-pages 0}

# Database statistics
(jbolt/db-stats db)
# => {:entries 42 :depth 2 :page-size 4096 :map-size 268435456 :last-page 5 :last-txn 10 :max-readers 126 :num-readers 0}
```

## Serialization

jbolt uses Janet's built-in `marshal`/`unmarshal` for all values:

- **Any Janet value is storable** — no type annotations, no schema definition.
- **Schema changes are free** — add or remove fields, old data is read as-is.
- **Compact binary format** — no JSON overhead.

```janet
(jbolt/put db "misc" "counter" 42)
(jbolt/put db "misc" "tags" @["foo" "bar" "baz"])
(jbolt/put db "misc" "config" {:debug true :max-connections 10})
(jbolt/put db "misc" "nested" {:user {:name "Axel"} :scores @[1 2 3]})
```

## Build from source

```sh
jpm build    # compile native module
jpm test     # run tests
jpm install  # install locally
```

## Project structure

```
jbolt/
  project.janet        # jpm build definition
  src/
    jbolt.c            # native C module (complete implementation)
  vendor/
    lmdb/
      mdb.c            # LMDB implementation
      midl.c           # LMDB internal (ID list)
      lmdb.h           # LMDB API header
      midl.h           # LMDB internal header
  test/
    test.janet
```

## Design decisions

- **Keys are always strings** — LMDB's byte-sorted order is directly usable.
- **Values are any Janet value** — serialized via `marshal`/`unmarshal`.
- **No nested buckets** — LMDB named databases are flat; complexity not worth it.
- **Everything in C** — transactions, iteration, and convenience functions are all implemented in the native module for clean lifecycle control and performance.

## Name

**jbolt** = **J**anet + **Bolt**. BoltDB was created in 2013 as a Go port of LMDB, later forked as bbolt. jbolt closes the circle: back to LMDB, with the bucket-oriented API that made Bolt beloved — now for Janet.

## License

MIT
