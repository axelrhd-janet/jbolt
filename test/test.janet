(import jbolt)
(import spork/test)
(import spork/json)

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

(test/assert
  (do
    (def db (fresh-db))
    (jbolt/ensure-bucket db "users")
    (def existing? (jbolt/has-bucket? db "users"))
    (def missing? (jbolt/has-bucket? db "nope"))
    (jbolt/close db)
    (cleanup)
    (and (= existing? true) (= missing? false)))
  "has-bucket? reports existence")

(test/assert
  (do
    (def db (fresh-db))
    # next-id auto-creates the meta bucket as a side effect
    (jbolt/next-id db "users")
    (def hidden? (jbolt/has-bucket? db "__jbolt_meta__"))
    (jbolt/close db)
    (cleanup)
    (= hidden? false))
  "has-bucket? hides reserved __jbolt_ buckets")

(test/assert
  (do
    (def db (fresh-db))
    (jbolt/ensure-bucket db "temp")
    (def before (jbolt/has-bucket? db "temp"))
    (jbolt/drop-bucket db "temp")
    (def after (jbolt/has-bucket? db "temp"))
    (jbolt/close db)
    (cleanup)
    (and (= before true) (= after false)))
  "has-bucket? sees drop-bucket")

(test/assert
  (do
    (def db (fresh-db))
    # Side-effect freedom: probing a missing bucket must not create it.
    (jbolt/has-bucket? db "ghost")
    (def bs (jbolt/buckets db))
    (jbolt/close db)
    (cleanup)
    (= 0 (length bs)))
  "has-bucket? does not create the bucket")

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
    (def seen @[])
    (jbolt/each db "items" (fn [k v] (array/push seen k)) :reverse true)
    (jbolt/close db)
    (cleanup)
    (deep= seen @["c" "b" "a"]))
  "each :reverse iterates in descending order")

(test/assert
  (do
    (def db (setup-iter-db))
    (def ks (jbolt/keys db "items" :reverse true))
    (def tuples (jbolt/collect db "items" :reverse true))
    (def mapped (jbolt/map db "items" (fn [k v] k) :reverse true))
    (def filtered (jbolt/filter db "items" (fn [k v] (> (v :n) 1)) :reverse true))
    (jbolt/close db)
    (cleanup)
    (and (deep= ks @["c" "b" "a"])
         (= "c" (get-in tuples [0 0]))
         (deep= mapped @["c" "b" "a"])
         (= "c" (get-in filtered [0 0]))))
  "keys/collect/map/filter support :reverse")

(test/assert
  (do
    (def db (setup-iter-db))
    (def last-key
      (jbolt/view db
        (fn [tx]
          (def ks (jbolt/tx-keys tx "items" :reverse true))
          (ks 0))))
    (jbolt/close db)
    (cleanup)
    (= last-key "c"))
  "tx-keys supports :reverse")

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

(test/assert
  (do
    (def db (setup-prefix-db))
    (def seen @[])
    (jbolt/each db "users"
      (fn [k v]
        (array/push seen k)
        (when (= k "usr-001") :break)))
    (jbolt/close db)
    (cleanup)
    (deep= seen @["adm-001" "usr-001"]))
  "each stops when callback returns :break")

(test/assert
  (do
    (def db (setup-prefix-db))
    (def seen @[])
    (jbolt/prefix db "users" "usr-"
      (fn [k v]
        (array/push seen k)
        (when (= k "usr-002") :break)))
    (jbolt/close db)
    (cleanup)
    (deep= seen @["usr-001" "usr-002"]))
  "prefix stops when callback returns :break")

(test/assert
  (do
    (def db (setup-prefix-db))
    (def seen @[])
    (jbolt/range db "users" "usr-001" "usr-003"
      (fn [k v]
        (array/push seen k)
        (when (= k "usr-002") :break)))
    (jbolt/close db)
    (cleanup)
    (deep= seen @["usr-001" "usr-002"]))
  "range stops when callback returns :break")

(test/assert
  (do
    (def db (setup-prefix-db))
    (def found
      (jbolt/view db
        (fn [tx]
          (var hit nil)
          (jbolt/tx-each tx "users"
            (fn [k v]
              (when (= (v :name) "Bob")
                (set hit [k v])
                :break)))
          hit)))
    (jbolt/close db)
    (cleanup)
    (and (= "usr-002" (found 0))
         (= "Bob" ((found 1) :name))))
  "tx-each stops on :break — find-first pattern")

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

(test/assert
  (do
    (def db (fresh-db))
    (jbolt/put db "data" "k1" "v1")
    (def results
      (jbolt/view db
        (fn [tx]
          [(jbolt/tx-has? tx "data" "k1")
           (jbolt/tx-has? tx "data" "missing")
           (jbolt/tx-has? tx "missing-bucket" "k")])))
    (jbolt/close db)
    (cleanup)
    (deep= results [true false false]))
  "tx-has? reports existence")

(test/assert
  (do
    (def db (fresh-db))
    (jbolt/ensure-bucket db "users")
    (jbolt/next-id db "users")
    (def results
      (jbolt/view db
        (fn [tx]
          [(jbolt/tx-has-bucket? tx "users")
           (jbolt/tx-has-bucket? tx "missing")
           (jbolt/tx-has-bucket? tx "__jbolt_meta__")])))
    (jbolt/close db)
    (cleanup)
    (deep= results [true false false]))
  "tx-has-bucket? reports existence and hides reserved buckets")

(test/assert
  (do
    (def db (fresh-db))
    # Inside a write txn, has-bucket? followed by ensure must agree atomically.
    (def created
      (jbolt/update db
        (fn [tx]
          (def before (jbolt/tx-has-bucket? tx "guarded"))
          (when (not before)
            (jbolt/tx-put tx "guarded" "k" "v"))
          before)))
    (def existing (jbolt/has-bucket? db "guarded"))
    (jbolt/close db)
    (cleanup)
    (and (= created false) (= existing true)))
  "tx-has-bucket? is consistent with subsequent tx-put inside one txn")

(test/assert
  (do
    (def db (fresh-db))
    (jbolt/put db "items" "a" {:n 1})
    (jbolt/put db "items" "b" {:n 2})
    (jbolt/put db "items" "c" {:n 3})
    (def result
      (jbolt/view db
        (fn [tx]
          {:count (jbolt/tx-count tx "items")
           :keys (jbolt/tx-keys tx "items")
           :collect (jbolt/tx-collect tx "items")
           :first (jbolt/tx-first tx "items")
           :last (jbolt/tx-last tx "items")
           :seek (jbolt/tx-seek tx "items" "b")})))
    (jbolt/close db)
    (cleanup)
    (and (= 3 (result :count))
         (deep= @["a" "b" "c"] (result :keys))
         (= 3 (length (result :collect)))
         (= "a" ((result :first) 0))
         (= "c" ((result :last) 0))
         (= "b" ((result :seek) 0))))
  "tx read cursor ops: count, keys, collect, first, last, seek")

(test/assert
  (do
    (def db (fresh-db))
    (jbolt/put db "items" "a" {:n 1})
    (jbolt/put db "items" "b" {:n 2})
    (jbolt/put db "items" "c" {:n 3})
    (def result
      (jbolt/view db
        (fn [tx]
          (def collected @[])
          (jbolt/tx-each tx "items" (fn [k v] (array/push collected k)))
          {:each collected
           :map (jbolt/tx-map tx "items" (fn [k v] (v :n)))
           :filter (jbolt/tx-filter tx "items" (fn [k v] (> (v :n) 1)))})))
    (jbolt/close db)
    (cleanup)
    (and (deep= @["a" "b" "c"] (result :each))
         (deep= @[1 2 3] (result :map))
         (= 2 (length (result :filter)))))
  "tx iteration callbacks: each, map, filter")

(test/assert
  (do
    (def db (fresh-db))
    (jbolt/put db "u" "usr-001" {:name "A"})
    (jbolt/put db "u" "usr-002" {:name "B"})
    (jbolt/put db "u" "adm-001" {:name "Z"})
    (def result
      (jbolt/view db
        (fn [tx]
          (def pfx @[])
          (jbolt/tx-prefix tx "u" "usr-" (fn [k v] (array/push pfx k)))
          (def rng @[])
          (jbolt/tx-range tx "u" "adm-001" "usr-001" (fn [k v] (array/push rng k)))
          {:prefix pfx :range rng})))
    (jbolt/close db)
    (cleanup)
    (and (deep= @["usr-001" "usr-002"] (result :prefix))
         (deep= @["adm-001" "usr-001"] (result :range))))
  "tx-prefix and tx-range scans")

(test/assert
  (do
    (def db (fresh-db))
    # atomic next-id-plus-put: allocate id and write the record in one txn
    (def record
      (jbolt/update db
        (fn [tx]
          (def id (jbolt/tx-next-id tx "items"))
          (def r {:id id :name "first"})
          (jbolt/tx-put tx "items" (string "item-" id) r)
          r)))
    (def stored (jbolt/get db "items" "item-1"))
    (def c (jbolt/count db "items"))
    (jbolt/close db)
    (cleanup)
    (and (= 1 (record :id))
         (deep= record stored)
         (= 1 c)))
  "tx-next-id enables atomic next-id-plus-put")

(test/assert
  (do
    (def db (fresh-db))
    (var caught false)
    (try
      (jbolt/view db
        (fn [tx]
          (jbolt/tx-next-id tx "items")))
      ([err] (set caught true)))
    (jbolt/close db)
    (cleanup)
    caught)
  "tx-next-id refuses read-only transaction")

(test/assert
  (do
    (def db (fresh-db))
    (jbolt/put db "data" "k" "v-old")
    # Compare-and-swap: only overwrite if old value matches
    (def swapped
      (jbolt/update db
        (fn [tx]
          (def current (jbolt/tx-get tx "data" "k"))
          (if (= current "v-old")
            (do (jbolt/tx-put tx "data" "k" "v-new") true)
            false))))
    (def v (jbolt/get db "data" "k"))
    (jbolt/close db)
    (cleanup)
    (and swapped (= v "v-new")))
  "compare-and-swap pattern using tx-get + tx-put")

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
    (jbolt/ensure-bucket db "items")
    (jbolt/next-id db "items")
    (jbolt/next-id db "items")
    (jbolt/put db "items" "a" {:n 1})
    (def c (jbolt/count db "items"))
    (def ks (jbolt/keys db "items"))
    (jbolt/close db)
    (cleanup)
    (and (= c 1) (deep= ks @["a"])))
  "next-id does not leak into count/keys of target bucket")

(test/assert
  (do
    (def db (fresh-db))
    (jbolt/ensure-bucket db "items")
    (jbolt/next-id db "items")
    (jbolt/put db "items" "a" {:n 1})
    (jbolt/put db "items" "b" {:n 2})
    (def entries (jbolt/collect db "items"))
    (def first-entry (jbolt/first db "items"))
    (def last-entry (jbolt/last db "items"))
    (var each-count 0)
    (jbolt/each db "items" (fn [k v] (++ each-count)))
    (jbolt/close db)
    (cleanup)
    (and (= 2 (length entries))
         (= "a" (first-entry 0))
         (= "b" (last-entry 0))
         (= 2 each-count)))
  "next-id does not leak into iteration")

(test/assert
  (do
    (def db (fresh-db))
    (jbolt/ensure-bucket db "items")
    (jbolt/next-id db "items")
    (def bs (jbolt/buckets db))
    (jbolt/close db)
    (cleanup)
    (and (= 1 (length bs))
         (= "items" (bs 0))))
  "meta bucket is hidden from (buckets db)")

(test/assert
  (do
    (def db (fresh-db))
    (jbolt/ensure-bucket db "items")
    (def id1 (jbolt/next-id db "items"))
    (jbolt/next-id db "items")
    (jbolt/drop-bucket db "items")
    (jbolt/ensure-bucket db "items")
    (def id-after (jbolt/next-id db "items"))
    (jbolt/close db)
    (cleanup)
    (and (= id1 1) (= id-after 1)))
  "drop-bucket resets next-id sequence")

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

# ----------------------------------------------------------------
# Export / Import
# ----------------------------------------------------------------

(test/start-suite "export-import")

(test/assert
  (do
    (def db (fresh-db))
    (jbolt/put db "items" "a" "v1")
    (jbolt/put db "items" "b" "v2")
    (jbolt/put db "items" "c" "v3")
    (def exported (jbolt/export-bucket db "items"))
    (jbolt/close db)
    (cleanup)
    (and (= 3 (length exported))
         (deep= ["a" "v1"] (exported 0))
         (deep= ["c" "v3"] (exported 2))))
  "export-bucket returns pair-array in key order")

(test/assert
  (do
    (def db (fresh-db))
    (def exported (jbolt/export-bucket db "missing"))
    (jbolt/close db)
    (cleanup)
    (and (= 0 (length exported))))
  "export-bucket on missing bucket returns empty array")

(test/assert
  (do
    (def db (fresh-db))
    (jbolt/put db "u" "u1" "v1")
    (jbolt/put db "p" "p1" 42)
    (def exported (jbolt/export-db db))
    (jbolt/close db)
    (cleanup)
    (and (= 2 (length (keys exported)))
         (deep= @[["u1" "v1"]] (exported "u"))
         (deep= @[["p1" 42]] (exported "p"))))
  "export-db returns table of bucket to pair-array")

(test/assert
  (do
    (def db (fresh-db))
    (jbolt/put db "items" "a" "v1")
    (jbolt/next-id db "items")  # writes to __jbolt_meta__
    (def without-meta (jbolt/export-db db))
    (def with-meta (jbolt/export-db db :include-meta true))
    (jbolt/close db)
    (cleanup)
    (and (nil? (without-meta "__jbolt_meta__"))
         (deep= @["items"] (keys without-meta))
         (not (nil? (with-meta "__jbolt_meta__")))))
  "export-db :include-meta controls meta bucket visibility")

(test/assert
  (do
    (def db (fresh-db))
    (jbolt/import-bucket db "items"
      @[["a" "v1"] ["b" "v2"] ["c" "v3"]])
    (def v (jbolt/get db "items" "b"))
    (def c (jbolt/count db "items"))
    (jbolt/close db)
    (cleanup)
    (and (= "v2" v) (= 3 c)))
  "import-bucket writes all pairs")

(test/assert
  (do
    (def db (fresh-db))
    # Mix tuples and arrays — simulate both Janet-native and json-decoded input
    (jbolt/import-bucket db "items"
      @[["a" 1] @["b" 2] ["c" 3]])
    (def c (jbolt/count db "items"))
    (jbolt/close db)
    (cleanup)
    (= 3 c))
  "import-bucket accepts tuples and arrays as inner pairs")

(test/assert
  (do
    (def db (fresh-db))
    (jbolt/put db "items" "a" "old")
    (jbolt/import-bucket db "items"
      @[["a" "new"] ["b" "fresh"]])
    (def va (jbolt/get db "items" "a"))
    (def vb (jbolt/get db "items" "b"))
    (jbolt/close db)
    (cleanup)
    (and (= va "new") (= vb "fresh")))
  "import-bucket overwrites existing keys")

(test/assert
  (do
    (def db (fresh-db))
    (jbolt/import-db db
      @{"users" @[["u1" "alice"] ["u2" "bob"]]
        "config" @[["theme" "dark"]]})
    (def uc (jbolt/count db "users"))
    (def v (jbolt/get db "config" "theme"))
    (jbolt/close db)
    (cleanup)
    (and (= 2 uc) (= v "dark")))
  "import-db writes multiple buckets")

(test/assert
  (do
    (def db (fresh-db))
    # Full roundtrip through Janet data
    (jbolt/put db "a" "x" "1")
    (jbolt/put db "a" "y" "2")
    (jbolt/put db "b" "z" 99)
    (def snap (jbolt/export-db db))
    (jbolt/drop-bucket db "a")
    (jbolt/drop-bucket db "b")
    (jbolt/import-db db snap)
    (def vx (jbolt/get db "a" "x"))
    (def vz (jbolt/get db "b" "z"))
    (jbolt/close db)
    (cleanup)
    (and (= vx "1") (= vz 99)))
  "export-db → import-db roundtrip")

(test/assert
  (do
    (def db (fresh-db))
    (jbolt/put db "existing" "kept" "intact")
    # Simulate input shape that json/decode would produce (all strings, arrays)
    (jbolt/import-db db
      {"imported" [["a" "1"] ["b" "2"]]})
    (def kept (jbolt/get db "existing" "kept"))
    (def imp (jbolt/get db "imported" "a"))
    (jbolt/close db)
    (cleanup)
    (and (= kept "intact") (= imp "1")))
  "import-db accepts struct input and leaves other buckets untouched")

(test/assert
  (do
    (def db (fresh-db))
    (jbolt/put db "data" "before" "value")
    (var caught false)
    (try
      # second entry is malformed — whole import must roll back
      (jbolt/import-db db
        @{"data" @[["ok" "value"] ["missing-value"]]})
      ([err] (set caught true)))
    (def still-there (jbolt/get db "data" "before"))
    (def new-key (jbolt/get db "data" "ok"))
    (jbolt/close db)
    (cleanup)
    (and caught
         (= still-there "value")
         (nil? new-key)))
  "import-db rolls back on malformed input")

(test/assert
  (do
    (def db (fresh-db))
    # JSON-safe values only (strings, numbers, booleans, JSON-compatible tables)
    (jbolt/put db "users" "u1" {"name" "Alice" "age" 30})
    (jbolt/put db "users" "u2" {"name" "Bob" "age" 25})
    (jbolt/put db "config" "theme" "dark")
    (def serialized (json/encode (jbolt/export-db db)))
    # Simulate restart via a fresh DB
    (jbolt/close db)
    (cleanup)
    (def db2 (fresh-db))
    (jbolt/import-db db2 (json/decode serialized))
    (def u1 (jbolt/get db2 "users" "u1"))
    (def theme (jbolt/get db2 "config" "theme"))
    (jbolt/close db2)
    (cleanup)
    (and (= "Alice" (u1 "name"))
         (= 30 (u1 "age"))
         (= "dark" theme)))
  "full JSON roundtrip: export → json/encode → json/decode → import")

(test/assert
  (do
    (def input @{"users" @[@["u1" @{"name" "Alice" "admin" true}]]})
    (def result (jbolt/keywordize-keys input))
    (and (not (nil? (result :users)))
         (nil? (result "users"))
         (= "Alice" (get-in result [:users 0 1 :name]))
         (= true (get-in result [:users 0 1 :admin]))
         # Values that happened to be strings stay strings
         (= "u1" (get-in result [:users 0 0]))))
  "keywordize-keys converts string keys but leaves values untouched")

(test/assert
  (do
    # Preserve container kinds
    (def t (jbolt/keywordize-keys @{"a" 1}))
    (def s (jbolt/keywordize-keys {"a" 1}))
    (def arr (jbolt/keywordize-keys @[@{"a" 1}]))
    (def tup (jbolt/keywordize-keys [{"a" 1}]))
    (and (table? t)
         (struct? s)
         (array? arr) (table? (arr 0))
         (tuple? tup) (struct? (tup 0))))
  "keywordize-keys preserves container kinds")

(test/assert
  (do
    (def db (fresh-db))
    # Simulate the real user workflow: dump via JSON, reload with keyword keys
    (jbolt/put db "users" "axelr" {:admin true :name "Axel"})
    (def dumped (json/encode (jbolt/export-db db)))
    (jbolt/close db)
    (cleanup)
    (def db2 (fresh-db))
    (jbolt/import-db db2
      (jbolt/keywordize-keys (json/decode dumped)))
    (def u (jbolt/get db2 "users" "axelr"))
    (jbolt/close db2)
    (cleanup)
    # After keywordize-keys, inner keys are keywords again
    (and (= "Axel" (u :name))
         (= true (u :admin))))
  "json/decode → keywordize-keys → import-db restores keyword keys")

(test/end-suite)
