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
| `each db bucket callback` | Über alle Einträge iterieren, callback erhält (k v) |
| `collect db bucket` | Alle Einträge als Array von [key value] Tupeln |
| `map db bucket callback` | Map über alle Einträge, callback erhält (k v) |
| `filter db bucket callback` | Einträge filtern, callback erhält (k v) |
| `keys db bucket` | Alle Keys als Array (ohne Values zu deserialisieren) |
| `first db bucket` | Erster Eintrag [key value] oder nil |
| `last db bucket` | Letzter Eintrag [key value] oder nil |
| `seek db bucket key` | Erster Eintrag mit Key >= gegebenem Key, oder nil |
| `prefix db bucket prefix callback` | Prefix-Scan: alle Keys die mit prefix beginnen |
| `range db bucket start end callback` | Range-Scan: Keys zwischen start und end (inklusiv) |
| `next-id db bucket` | Auto-Increment-Sequenz pro Bucket (intern via reserviertem Key `__seq__`) |
| `update db callback` | Read-Write-Transaktion: callback erhält tx, commit bei Erfolg, rollback bei Error |
| `view db callback` | Read-Only-Transaktion: callback erhält tx |
| `tx-put tx bucket key value` | Put innerhalb einer expliziten Transaktion |
| `tx-get tx bucket key` | Get innerhalb einer expliziten Transaktion |
| `tx-delete tx bucket key` | Delete innerhalb einer expliziten Transaktion |
| `backup db path` | Konsistenter Snapshot der DB in eine Datei (via mdb_env_copy2) |
| `stats db bucket` | Bucket-Statistiken via mdb_stat (Einträge, Tiefe, Page-Größe) |
| `db-stats db` | DB-Statistiken via mdb_env_stat + mdb_env_info (Map-Size, Größe, Max Readers) |

### Designentscheidungen

- **Keys sind immer Strings** — bewusste Einschränkung, damit LMDB-Sortierung direkt nutzbar bleibt
- **Values sind beliebige Janet-Werte** — Serialisierung via Janet marshal/unmarshal
- **Keine nested Buckets** — LMDB named databases sind flach, Komplexität lohnt sich nicht
- **Transactions in C** — update/view direkt in C implementieren, nicht im Janet-Wrapper, damit der Transaction-Lifecycle (begin/commit/abort) sauber kontrolliert wird
- **Iteration in C** — each/collect/map/filter nutzen LMDB-Cursor direkt in C für Performance
- **map-size muss konfigurierbar sein** — LMDB braucht vorab definierte Max-Größe, MDB_MAP_FULL ist der häufigste Fehler in der Praxis

### Error-Handling

LMDB-Fehlercodes als Janet-Errors mit klaren Messages werfen, z.B.:
- `"jbolt: database full (MDB_MAP_FULL) — increase :map-size"`
- `"jbolt: bucket not found: users"`
- `"jbolt: database already open by another process"`

### bbolt-Features die wir bewusst weglassen

- Nested Buckets, MoveBucket — LMDB ist flach
- Cursor.Delete() — jbolt/delete reicht
- Tx.OnCommit() — Over-Engineering
- DB.Batch() — evtl. später, nicht V1
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
