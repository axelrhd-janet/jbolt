# jbolt

Embedded, schema-freier Key-Value Store für Janet, basierend auf LMDB. Inspiriert von bbolt (Go), aber zurück zu LMDB als Storage Engine mit Janet marshal/unmarshal für Serialisierung.

## Architektur

- **Native C-Modul** (`src/jbolt.c`) wrapping LMDB via Janet Abstract Types — alles in einer Datei, kein separater Janet-Wrapper
- **LMDB vendored** in `vendor/lmdb/` (mdb.c, midl.c, lmdb.h, midl.h) — wird mit dem Modul kompiliert, keine externen Abhängigkeiten
- Build via `jpm` (project.janet)

## API-Übersicht

### Kern (C-Modul)

| Funktion | Beschreibung |
|---|---|
| `open path &named :max-buckets :map-size :mode` | DB öffnen/erstellen. Defaults: max-buckets=16, map-size=256MB, mode=0664 |
| `close db` | DB schließen |
| `ensure-bucket db name` | Bucket anlegen falls nicht vorhanden |
| `drop-bucket db name` | Bucket mit allen Inhalten löschen |
| `buckets db` | Alle Bucket-Namen als Array |
| `put db bucket key value` | Wert speichern (Key=String, Value=beliebiger Janet-Wert via marshal) |
| `get db bucket key` | Wert lesen (nil wenn nicht vorhanden) |
| `delete db bucket key` | Key löschen |
| `has? db bucket key` | Prüfen ob Key existiert |
| `count db bucket` | Anzahl Einträge via mdb_stat |
| `each db bucket callback &named :reverse` | Über alle Einträge iterieren. Callback kann `:break` zurückgeben |
| `collect db bucket &named :reverse` | Alle Einträge als Array von [key value] Tupeln |
| `map db bucket callback &named :reverse` | Map über alle Einträge |
| `filter db bucket callback &named :reverse` | Einträge filtern (truthy callback → include) |
| `keys db bucket &named :reverse` | Alle Keys als Array (ohne Values zu deserialisieren) |
| `first db bucket` | Erster Eintrag [key value] oder nil |
| `last db bucket` | Letzter Eintrag [key value] oder nil |
| `seek db bucket key` | Erster Eintrag mit Key >= gegebenem Key, oder nil |
| `prefix db bucket prefix callback` | Prefix-Scan. Callback kann `:break` zurückgeben |
| `range db bucket start end callback` | Range-Scan (inklusiv). Callback kann `:break` zurückgeben |
| `next-id db bucket` | Auto-Increment-Sequenz pro Bucket (State in reserviertem Meta-Bucket `__jbolt_meta__`) |
| `update db callback` | Read-Write-Transaktion: callback erhält tx, commit bei Erfolg, rollback bei Error |
| `view db callback` | Read-Only-Transaktion: callback erhält tx |
| `tx-put tx bucket key value` | Put innerhalb einer expliziten Transaktion |
| `tx-get tx bucket key` | Get innerhalb einer expliziten Transaktion |
| `tx-delete tx bucket key` | Delete innerhalb einer expliziten Transaktion |
| `tx-has? tx bucket key` | Prüfen ob Key existiert, innerhalb einer Transaktion |
| `tx-count tx bucket` | Anzahl Einträge innerhalb einer Transaktion |
| `tx-keys tx bucket &named :reverse` | Alle Keys innerhalb einer Transaktion |
| `tx-collect tx bucket &named :reverse` | Alle Einträge als Tupel innerhalb einer Transaktion |
| `tx-each tx bucket f &named :reverse` | Iteration innerhalb einer Transaktion, `:break` möglich |
| `tx-map tx bucket f &named :reverse` | Map innerhalb einer Transaktion |
| `tx-filter tx bucket f &named :reverse` | Filter innerhalb einer Transaktion |
| `tx-first tx bucket` | Erster Eintrag innerhalb einer Transaktion |
| `tx-last tx bucket` | Letzter Eintrag innerhalb einer Transaktion |
| `tx-seek tx bucket key` | Erster Eintrag mit Key >= innerhalb einer Transaktion |
| `tx-prefix tx bucket prefix f` | Prefix-Scan innerhalb einer Transaktion, `:break` möglich |
| `tx-range tx bucket start end f` | Range-Scan innerhalb einer Transaktion, `:break` möglich |
| `tx-next-id tx bucket` | Auto-Increment innerhalb einer RW-Transaktion (für atomic next-id-plus-put) |
| `backup db path` | Konsistenter Snapshot der DB in eine Datei (via mdb_env_copy2) |
| `stats db bucket` | Bucket-Statistiken via mdb_stat (Einträge, Tiefe, Page-Größe) |
| `db-stats db` | DB-Statistiken via mdb_env_stat + mdb_env_info (Map-Size, Größe, Max Readers) |
| `export-bucket db bucket` | Bucket als `@[[k v] ...]` Pair-Array in Key-Order |
| `export-db db &named :include-meta` | DB als `@{bucket entries}` Table. `:include-meta true` schließt `__jbolt_meta__` ein |
| `import-bucket db bucket entries` | Schreibt Pair-Array in Bucket (overwrite). Inner pairs: Tuple oder Array |
| `import-db db data` | Schreibt Table/Struct `{bucket entries}` in einer Write-Txn (atomic). Bucket-Namen dürfen Strings oder Keywords sein |
| `keywordize-keys data` | Rekursiver Helper: String-Keys in Tables/Structs werden zu Keywords. Values bleiben unberührt. Gedacht für Post-Processing nach `json/decode` |

### Designentscheidungen

- **Keys sind immer Strings** — bewusste Einschränkung, damit LMDB-Sortierung direkt nutzbar bleibt
- **Values sind beliebige Janet-Werte** — Serialisierung via Janet marshal/unmarshal
- **Keine nested Buckets** — LMDB named databases sind flach, Komplexität lohnt sich nicht
- **Transactions in C** — update/view direkt in C implementieren, nicht im Janet-Wrapper, damit der Transaction-Lifecycle (begin/commit/abort) sauber kontrolliert wird
- **Iteration in C** — each/collect/map/filter nutzen LMDB-Cursor direkt in C für Performance
- **map-size muss konfigurierbar sein** — LMDB braucht vorab definierte Max-Größe, MDB_MAP_FULL ist der häufigste Fehler in der Praxis
- **`:break` statt truthy-Return für Iteration-Abbruch** — viele User-Callbacks nutzen `array/push` o.Ä., dessen truthy Return sonst ungewollt abbrechen würde
- **Meta-Bucket `__jbolt_meta__`** — reserviert für internen State (z.B. `next-id`-Sequenzen). Mit `__jbolt_`-Prefix versteckt aus `buckets`. Keine Kollision mit User-Buckets
- **Kein `put-many`** — `update` + `tx-put`-Loop ist bereits der idiomatische Bulk-Insert (eine Txn, ein Commit). Keine zusätzliche API nötig
- **Export liefert Janet-Werte, kein JSON** — `export-db`/`export-bucket` geben rohe Janet-Strukturen zurück. JSON-Encoding ist User-Verantwortung (typisch via `spork/json`). Der Hinweis auf JSON-Lossiness steht in der README
- **Export-DB atomar via Read-Txn, Import-DB atomar via Write-Txn** — `export-db` ist Snapshot-konsistent auch wenn parallel geschrieben wird; `import-db` ist all-or-nothing, sodass malformed Input keine halben Zustände hinterlässt

### Error-Handling

LMDB-Fehlercodes als Janet-Errors mit klaren Messages werfen, z.B.:
- `"jbolt: database full (MDB_MAP_FULL) — increase :map-size"`
- `"jbolt: bucket not found: users"`
- `"jbolt: database already open by another process"`

### bbolt-Features die wir bewusst weglassen

- Nested Buckets, MoveBucket — LMDB ist flach
- Cursor.Delete() — jbolt/delete reicht
- Tx.OnCommit() — Over-Engineering
- DB.Batch() / put-many — `update` + `tx-put`-Loop erfüllt das bereits
- NoSync, StrictMode, FillPercent — Low-level Tuning das Janet-User nicht brauchen
- Tx.Check() — LMDB ist robuster als bbolt, nicht nötig

## Projektstruktur

```
jbolt/
  project.janet        # jpm Build-Definition
  CLAUDE.md            # diese Datei
  src/
    jbolt.c            # Native C-Modul (komplette Implementierung)
  vendor/
    lmdb/
      mdb.c            # LMDB-Implementierung
      midl.c           # LMDB Internal (ID-Liste)
      lmdb.h           # LMDB-Header
      midl.h           # LMDB Internal Header
  test/
    test.janet
```

## Build & Test

```fish
jpm build              # Native Modul kompilieren
jpm test               # Tests ausführen
```
