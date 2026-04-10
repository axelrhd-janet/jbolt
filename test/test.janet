(import jbolt)
(import spork/test)

(def test-db-path "/tmp/jbolt-test.mdb")
(def test-backup-path "/tmp/jbolt-test-backup.mdb")

(defn cleanup []
  (each f [test-db-path
           (string test-db-path "-lock")
           test-backup-path
           (string test-backup-path "-lock")]
    (when (os/stat f)
      (os/rm f))))

(defn fresh-db []
  (cleanup)
  (jbolt/open test-db-path))

# ----------------------------------------------------------------
# Phase 1: Lifecycle
# ----------------------------------------------------------------

(test/start-suite "lifecycle")

(test/assert
  (do
    (def db (fresh-db))
    (jbolt/close db)
    (cleanup)
    true)
  "open and close")

(test/assert
  (do
    (def db (fresh-db))
    (jbolt/close db)
    (jbolt/close db)  # double close should be safe
    (cleanup)
    true)
  "double close is safe")

(test/assert
  (do
    (def db (jbolt/open test-db-path :max-buckets 4 :map-size 1048576 :mode 0644))
    (jbolt/close db)
    (cleanup)
    true)
  "open with named params")

(test/end-suite)

# ----------------------------------------------------------------
# Phase 1: Bucket management
# ----------------------------------------------------------------

(test/start-suite "buckets")

(test/assert
  (do
    (def db (fresh-db))
    (jbolt/ensure-bucket db "users")
    (jbolt/ensure-bucket db "sessions")
    (def bs (jbolt/buckets db))
    (jbolt/close db)
    (cleanup)
    (and (= 2 (length bs))
         (not= nil (find |(= $ "users") bs))
         (not= nil (find |(= $ "sessions") bs))))
  "ensure-bucket and list buckets")

(test/assert
  (do
    (def db (fresh-db))
    (jbolt/ensure-bucket db "temp")
    (jbolt/drop-bucket db "temp")
    (def bs (jbolt/buckets db))
    (jbolt/close db)
    (cleanup)
    (= 0 (length bs)))
  "drop-bucket removes bucket")

(test/assert
  (do
    (def db (fresh-db))
    (jbolt/ensure-bucket db "x")
    (jbolt/ensure-bucket db "x")  # idempotent
    (def bs (jbolt/buckets db))
    (jbolt/close db)
    (cleanup)
    (= 1 (length bs)))
  "ensure-bucket is idempotent")

(test/end-suite)

# ----------------------------------------------------------------
# Phase 1: CRUD
# ----------------------------------------------------------------

(test/start-suite "crud")

(test/assert
  (do
    (def db (fresh-db))
    (jbolt/put db "data" "key1" "hello")
    (def v (jbolt/get db "data" "key1"))
    (jbolt/close db)
    (cleanup)
    (= v "hello"))
  "put and get string")

(test/assert
  (do
    (def db (fresh-db))
    (jbolt/put db "data" "num" 42)
    (def v (jbolt/get db "data" "num"))
    (jbolt/close db)
    (cleanup)
    (= v 42))
  "put and get number")

(test/assert
  (do
    (def db (fresh-db))
    (jbolt/put db "data" "tbl" {:name "Axel" :role :admin})
    (def v (jbolt/get db "data" "tbl"))
    (jbolt/close db)
    (cleanup)
    (and (= (v :name) "Axel")
         (= (v :role) :admin)))
  "put and get table")

(test/assert
  (do
    (def db (fresh-db))
    (jbolt/put db "data" "arr" @[1 2 3])
    (def v (jbolt/get db "data" "arr"))
    (jbolt/close db)
    (cleanup)
    (deep= v @[1 2 3]))
  "put and get array")

(test/assert
  (do
    (def db (fresh-db))
    (jbolt/put db "data" "bool" true)
    (def v (jbolt/get db "data" "bool"))
    (jbolt/close db)
    (cleanup)
    (= v true))
  "put and get boolean")

(test/assert
  (do
    (def db (fresh-db))
    (jbolt/put db "data" "kw" :test-keyword)
    (def v (jbolt/get db "data" "kw"))
    (jbolt/close db)
    (cleanup)
    (= v :test-keyword))
  "put and get keyword")

(test/assert
  (do
    (def db (fresh-db))
    (jbolt/put db "data" "nested" {:user {:name "Axel"} :scores @[1 2 3]})
    (def v (jbolt/get db "data" "nested"))
    (jbolt/close db)
    (cleanup)
    (and (= ((v :user) :name) "Axel")
         (deep= (v :scores) @[1 2 3])))
  "put and get nested structure")

(test/assert
  (do
    (def db (fresh-db))
    (def v (jbolt/get db "nonexistent" "key"))
    (jbolt/close db)
    (cleanup)
    (nil? v))
  "get from nonexistent bucket returns nil")

(test/assert
  (do
    (def db (fresh-db))
    (jbolt/put db "data" "key1" "val")
    (def v (jbolt/get db "data" "missing"))
    (jbolt/close db)
    (cleanup)
    (nil? v))
  "get missing key returns nil")

(test/assert
  (do
    (def db (fresh-db))
    (jbolt/put db "data" "key1" "val")
    (jbolt/delete db "data" "key1")
    (def v (jbolt/get db "data" "key1"))
    (jbolt/close db)
    (cleanup)
    (nil? v))
  "delete removes key")

(test/assert
  (do
    (def db (fresh-db))
    (jbolt/put db "data" "key1" "val")
    (def h1 (jbolt/has? db "data" "key1"))
    (def h2 (jbolt/has? db "data" "missing"))
    (jbolt/close db)
    (cleanup)
    (and h1 (not h2)))
  "has? returns correct boolean")

(test/assert
  (do
    (def db (fresh-db))
    (jbolt/put db "data" "key1" "v1")
    (jbolt/put db "data" "key1" "v2")
    (def v (jbolt/get db "data" "key1"))
    (jbolt/close db)
    (cleanup)
    (= v "v2"))
  "put overwrites existing value")

(test/assert
  (do
    (def db (fresh-db))
    (jbolt/put db "data" "u1" {:name "Axel" :role :user})
    (jbolt/merge db "data" "u1" {:email "axel@example.com" :role :admin})
    (def v (jbolt/get db "data" "u1"))
    (jbolt/close db)
    (cleanup)
    (and (= (v :name) "Axel")
         (= (v :email) "axel@example.com")
         (= (v :role) :admin)))
  "merge adds and overwrites fields")

(test/assert
  (do
    (def db (fresh-db))
    (jbolt/put db "data" "u1" {:name "Axel" :tmp "remove-me" :other "also-remove"})
    (jbolt/dissoc db "data" "u1" :tmp :other)
    (def v (jbolt/get db "data" "u1"))
    (jbolt/close db)
    (cleanup)
    (and (= (v :name) "Axel")
         (nil? (v :tmp))
         (nil? (v :other))))
  "dissoc removes fields")

(test/assert
  (do
    (def db (fresh-db))
    (jbolt/ensure-bucket db "data")
    (def result (jbolt/merge db "data" "new-key" {:name "New"}))
    (def v (jbolt/get db "data" "new-key"))
    (jbolt/close db)
    (cleanup)
    (and (= (v :name) "New")
         (= (result :name) "New")))
  "merge creates entry if key does not exist")

(test/end-suite)

# ----------------------------------------------------------------
# Phase 2: Iteration
# ----------------------------------------------------------------

(test/start-suite "iteration")

(defn setup-iter-db []
  (def db (fresh-db))
  (jbolt/ensure-bucket db "items")
  (jbolt/put db "items" "a" {:n 1})
  (jbolt/put db "items" "b" {:n 2})
  (jbolt/put db "items" "c" {:n 3})
  db)

(test/assert
  (do
    (def db (setup-iter-db))
    (var count 0)
    (jbolt/each db "items" (fn [k v] (++ count)))
    (jbolt/close db)
    (cleanup)
    (= count 3))
  "each iterates all entries")

(test/assert
  (do
    (def db (setup-iter-db))
    (def result (jbolt/collect db "items"))
    (jbolt/close db)
    (cleanup)
    (and (= 3 (length result))
         (= "a" (get-in result [0 0]))
         (= 1 (get-in result [0 1 :n]))))
  "collect returns all entries as tuples")

(test/assert
  (do
    (def db (setup-iter-db))
    (def result (jbolt/map db "items" (fn [k v] (v :n))))
    (jbolt/close db)
    (cleanup)
    (deep= result @[1 2 3]))
  "map transforms entries")

(test/assert
  (do
    (def db (setup-iter-db))
    (def result (jbolt/filter db "items" (fn [k v] (> (v :n) 1))))
    (jbolt/close db)
    (cleanup)
    (and (= 2 (length result))
         (= "b" (get-in result [0 0]))
         (= "c" (get-in result [1 0]))))
  "filter selects matching entries")

(test/assert
  (do
    (def db (setup-iter-db))
    (def result (jbolt/keys db "items"))
    (jbolt/close db)
    (cleanup)
    (deep= result @["a" "b" "c"]))
  "keys returns all keys in order")

(test/assert
  (do
    (def db (setup-iter-db))
    (def c (jbolt/count db "items"))
    (jbolt/close db)
    (cleanup)
    (= c 3))
  "count returns number of entries")

(test/assert
  (do
    (def db (fresh-db))
    (jbolt/ensure-bucket db "empty")
    (def c (jbolt/count db "empty"))
    (jbolt/close db)
    (cleanup)
    (= c 0))
  "count returns 0 for empty bucket")

(test/assert
  (do
    (def db (setup-iter-db))
    (def f (jbolt/first db "items"))
    (jbolt/close db)
    (cleanup)
    (and (= "a" (f 0))
         (= 1 ((f 1) :n))))
  "first returns first entry")

(test/assert
  (do
    (def db (setup-iter-db))
    (def l (jbolt/last db "items"))
    (jbolt/close db)
    (cleanup)
    (and (= "c" (l 0))
         (= 3 ((l 1) :n))))
  "last returns last entry")

(test/assert
  (do
    (def db (fresh-db))
    (jbolt/ensure-bucket db "empty")
    (def f (jbolt/first db "empty"))
    (def l (jbolt/last db "empty"))
    (jbolt/close db)
    (cleanup)
    (and (nil? f) (nil? l)))
  "first/last return nil for empty bucket")

(test/assert
  (do
    (def db (setup-iter-db))
    (def s (jbolt/seek db "items" "b"))
    (jbolt/close db)
    (cleanup)
    (and (= "b" (s 0))
         (= 2 ((s 1) :n))))
  "seek finds exact key")

(test/assert
  (do
    (def db (setup-iter-db))
    (def s (jbolt/seek db "items" "bb"))
    (jbolt/close db)
    (cleanup)
    (and (= "c" (s 0))))
  "seek finds next key >= given")

(test/assert
  (do
    (def db (setup-iter-db))
    (def s (jbolt/seek db "items" "z"))
    (jbolt/close db)
    (cleanup)
    (nil? s))
  "seek returns nil when no key >= given")

(test/end-suite)

# ----------------------------------------------------------------
# Phase 2: prefix and range
# ----------------------------------------------------------------

(test/start-suite "prefix-range")

(defn setup-prefix-db []
  (def db (fresh-db))
  (jbolt/ensure-bucket db "users")
  (jbolt/put db "users" "usr-001" {:name "Alice"})
  (jbolt/put db "users" "usr-002" {:name "Bob"})
  (jbolt/put db "users" "usr-003" {:name "Charlie"})
  (jbolt/put db "users" "adm-001" {:name "Admin"})
  db)

(test/assert
  (do
    (def db (setup-prefix-db))
    (def results @[])
    (jbolt/prefix db "users" "usr-"
      (fn [k v] (array/push results k)))
    (jbolt/close db)
    (cleanup)
    (deep= results @["usr-001" "usr-002" "usr-003"]))
  "prefix scans matching keys")

(test/assert
  (do
    (def db (setup-prefix-db))
    (def results @[])
    (jbolt/prefix db "users" "zzz"
      (fn [k v] (array/push results k)))
    (jbolt/close db)
    (cleanup)
    (= 0 (length results)))
  "prefix with no matches returns empty")

(test/assert
  (do
    (def db (setup-prefix-db))
    (def results @[])
    (jbolt/range db "users" "usr-001" "usr-002"
      (fn [k v] (array/push results k)))
    (jbolt/close db)
    (cleanup)
    (deep= results @["usr-001" "usr-002"]))
  "range scans inclusive bounds")

(test/assert
  (do
    (def db (setup-prefix-db))
    (def results @[])
    (jbolt/range db "users" "adm-001" "usr-001"
      (fn [k v] (array/push results k)))
    (jbolt/close db)
    (cleanup)
    (deep= results @["adm-001" "usr-001"]))
  "range across different prefixes")

(test/end-suite)

# ----------------------------------------------------------------
# Phase 3: Transactions
# ----------------------------------------------------------------

(test/start-suite "transactions")

(test/assert
  (do
    (def db (fresh-db))
    (jbolt/ensure-bucket db "data")
    (jbolt/update db
      (fn [tx]
        (jbolt/tx-put tx "data" "k1" "v1")
        (jbolt/tx-put tx "data" "k2" "v2")))
    (def v1 (jbolt/get db "data" "k1"))
    (def v2 (jbolt/get db "data" "k2"))
    (jbolt/close db)
    (cleanup)
    (and (= v1 "v1") (= v2 "v2")))
  "update commits atomically")

(test/assert
  (do
    (def db (fresh-db))
    (jbolt/ensure-bucket db "data")
    (jbolt/put db "data" "existing" "before")
    (try
      (jbolt/update db
        (fn [tx]
          (jbolt/tx-put tx "data" "existing" "after")
          (error "rollback!")))
      ([err] nil))
    (def v (jbolt/get db "data" "existing"))
    (jbolt/close db)
    (cleanup)
    (= v "before"))
  "update rolls back on error")

(test/assert
  (do
    (def db (fresh-db))
    (jbolt/ensure-bucket db "data")
    (jbolt/put db "data" "k1" "v1")
    (def result
      (jbolt/view db
        (fn [tx]
          (jbolt/tx-get tx "data" "k1"))))
    (jbolt/close db)
    (cleanup)
    (= result "v1"))
  "view reads data")

(test/assert
  (do
    (def db (fresh-db))
    (jbolt/ensure-bucket db "data")
    (jbolt/update db
      (fn [tx]
        (jbolt/tx-put tx "data" "k1" "v1")
        (def v (jbolt/tx-get tx "data" "k1"))
        (jbolt/tx-delete tx "data" "k1")
        v))
    (def v (jbolt/get db "data" "k1"))
    (jbolt/close db)
    (cleanup)
    (nil? v))
  "tx-delete within transaction")

(test/end-suite)

# ----------------------------------------------------------------
# Phase 4: Utility
# ----------------------------------------------------------------

(test/start-suite "utility")

(test/assert
  (do
    (def db (fresh-db))
    (jbolt/ensure-bucket db "items")
    (def id1 (jbolt/next-id db "items"))
    (def id2 (jbolt/next-id db "items"))
    (def id3 (jbolt/next-id db "items"))
    (jbolt/close db)
    (cleanup)
    (and (= id1 1) (= id2 2) (= id3 3)))
  "next-id returns sequential values")

(test/assert
  (do
    (def db (fresh-db))
    (jbolt/ensure-bucket db "data")
    (jbolt/put db "data" "key" "value")
    (jbolt/backup db test-backup-path)
    (jbolt/close db)
    (def exists (not (nil? (os/stat test-backup-path))))
    # Verify backup is usable
    (def db2 (jbolt/open test-backup-path))
    (def v (jbolt/get db2 "data" "key"))
    (jbolt/close db2)
    (cleanup)
    (and exists (= v "value")))
  "backup creates usable copy")

(test/assert
  (do
    (def db (fresh-db))
    (jbolt/ensure-bucket db "data")
    (jbolt/put db "data" "k1" "v1")
    (jbolt/put db "data" "k2" "v2")
    (def s (jbolt/stats db "data"))
    (jbolt/close db)
    (cleanup)
    (and (= (s :entries) 2)
         (> (s :page-size) 0)))
  "stats returns bucket statistics")

(test/assert
  (do
    (def db (fresh-db))
    (def s (jbolt/db-stats db))
    (jbolt/close db)
    (cleanup)
    (and (> (s :map-size) 0)
         (> (s :page-size) 0)
         (> (s :max-readers) 0)))
  "db-stats returns database statistics")

(test/end-suite)

# ----------------------------------------------------------------
# Error cases
# ----------------------------------------------------------------

(test/start-suite "errors")

(test/assert
  (do
    (def db (fresh-db))
    (jbolt/close db)
    (var caught false)
    (try
      (jbolt/put db "data" "k" "v")
      ([err] (set caught true)))
    (cleanup)
    caught)
  "operating on closed db raises error")

(test/assert
  (do
    (def db (fresh-db))
    (var caught false)
    (try
      (jbolt/drop-bucket db "nonexistent")
      ([err] (set caught true)))
    (jbolt/close db)
    (cleanup)
    caught)
  "dropping nonexistent bucket raises error")

(test/end-suite)
