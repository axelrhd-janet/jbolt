(declare-project
  :name "jbolt"
  :description "Embedded key-value store for Janet, wrapping LMDB"
  :version "0.1.0")

(declare-native
  :name "jbolt"
  :source @["vendor/lmdb/mdb.c" "vendor/lmdb/midl.c" "src/jbolt.c"]
  :cflags @["-Ivendor/lmdb" "-pthread"]
  :lflags @["-pthread"])
