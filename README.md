# DynamicModel – Mini‑ORM für PostgreSQL (psycopg2)

Ein leichtgewichtiges, produktionsnahes Mini‑ORM mit:
- CRUD, Upsert, Bulk‑Operationen
- Transaktionen + Savepoints (verschachtelbar)
- Soft‑Delete (automatische Filterung)
- Hooks (global/Tabellen-spezifisch)
- Migrations-Framework mit Historie
- DDL‑Helper (Index, FK, Constraints, Timestamps)
- Audit‑Trail via Trigger
- Logger, Streaming, Aggregates, Schema‑Cache, Typ‑Inference

Kompatibel mit Python 3.8+ und PostgreSQL (psycopg2).



## Inhalt
- Installation
- Schnellstart
- Verbindungsmanagement
- Logging
- Transaktionen und Savepoints
- Schema & DDL
- CRUD & Querying
- Instanzen & Attribute
- Soft‑Delete
- Relationen
- Migrationen
- Audit‑Trail
- Raw SQL, Streaming, Explain
- Utilities
- Hooks
- Schema‑Caching & Typ‑Inference
- Performance‑Tipps
- Sicherheit & Fehlerverhalten
- FAQ



## Installation

Voraussetzungen:
- Python 3.8+
- PostgreSQL
- psycopg2

Installation:
```bash
pip install psycopg2  # oder psycopg2-binary
```



## Schnellstart

```python
from dynamic_model import DynamicModel as DM

# 1) Verbindung herstellen (Einzelverbindung oder Pool)
DM.connect(host="localhost", dbname="app", user="app", password="secret")
# Alternativ: DM.connect_pool(minconn=1, maxconn=10, host=..., ...)

# 2) Tabelle anlegen
DM.create_table("users", {
    "email": "TEXT NOT NULL",
    "name": "TEXT",
})

# 3) Insert (fehlende Spalten werden mit sinnvollem Typ ergänzt)
u = DM.create("users", email="a@b.c", name="Ada", is_admin=True)

# 4) Lesen
first = DM.first("users")
all_users = DM.get_all("users", order_by=("-id",))

# 5) Update
u.name = "Ada L."
u.save()

# 6) Transaktion
with DM.transaction():
    u2 = DM.create("users", email="x@y.z")
    # savepoint optional:
    with DM.savepoint():
        pass

# 7) Soft-Delete
u.soft_delete()
active = DM.get_all("users")  # standardmäßig werden gelöschte ausgeblendet

# 8) Schließen
DM.close()
```



## Verbindungsmanagement

- `connect(**db_params)`: Einzelverbindung (autocommit=False).
- `connect_pool(minconn=1, maxconn=5, **db_params)`: Connection Pool.
- `close()`/`close_pool()`: schließt Verbindung/Pool sauber.
- `healthcheck() -> bool`: SELECT 1, prüft Erreichbarkeit.

Tipp: Bei Streaming (serverseitiger Cursor) wird eine Verbindung so lange belegt, bis das Iterieren abgeschlossen ist.



## Logging

- `set_logger(fn)`: Setzt optionalen Logger, Signatur: `fn(sql_text, params)`.
- Alle Queries werden bestmöglich in Stringform (inkl. Parameterliste) geloggt.
- Logger darf keine Exceptions auslösen (werden abgefangen).

Beispiel:
```python
DM.set_logger(lambda q, p: print(q, p))
```



## Transaktionen und Savepoints

- `transaction()`: Context‑Manager, verschachtelbar. Innere Ebenen setzen Savepoints.
- `savepoint(name=None)`: eigener Savepoint‑Context; muss innerhalb `transaction()` genutzt werden.

Beispiel:
```python
with DM.transaction():
    # ... DB-Operationen
    with DM.savepoint():
        # Teilschritt, der bei Fehlern separat zurückgerollt wird
        pass
```



## Schema & DDL

- `create_table(table, schema: Dict[str, str])`: legt Tabelle an (id SERIAL PK automatisch).
- `drop_table(table, cascade=False)`: löscht Tabelle.
- `inspect_schema(table)`: Spaltenmetadaten (mit Cache).
- `set_schema_cache_ttl(seconds)`: TTL in Sekunden (0 = kein Ablauf, d. h. dauerhafter Cache).
- `ensure_columns(table, columns: Dict[str, str])`: mehrere Spalten hinzufügen (nur falls fehlend).
- `add_index(table, column, unique=False)`
- `add_unique(table, cols, name=None)`
- `drop_constraint(table, name)`
- `add_foreign_key(table, column, ref_table, ref_column="id", on_delete="CASCADE", name=None)`
- `drop_foreign_key(table, name)`
- `rename_column(table, old, new)`
- `drop_column(table, column)`
- `add_timestamps(table)`: created_at/updated_at + Trigger zum Aktualisieren von updated_at.
- `ensure_version_column(table, version_col="version")`: für Optimistic Locking.

Hinweis: `add_timestamps` nutzt PL/pgSQL; die Sprache ist in PostgreSQL standardmäßig verfügbar.



## CRUD & Querying

Soft‑Deleted‑Datensätze werden standardmäßig ausgeblendet (`exclude_deleted=True`). Setze `exclude_deleted=False`, um das zu deaktivieren. Der Filter greift nur, wenn die Spalte `deleted` existiert.

- Insert:
  - `create(table, **kwargs) -> DynamicModel`
  - `bulk_create(table, rows: List[Dict]) -> List[int]`
  - `upsert(table, conflict_cols, values: Dict, update_cols=None) -> int` (RETURNING id)
  - `get_or_create(table, defaults=None, **conditions) -> (obj, created_bool)`

- Select IDs / Listen:
  - `find_ids(table, order_by=(), limit=None, offset=None, exclude_deleted=True, **conditions) -> List[int]`
  - `list_all_ids(table, exclude_deleted=True) -> List[int]`

- Select Objekte:
  - `get_all(table, order_by=(), exclude_deleted=True, **conditions) -> List[DynamicModel]`
  - `paginate(table, page=1, per_page=25, order_by=(), exclude_deleted=True, **conditions) -> List[DynamicModel]`
  - `paginate_with_count(table, page, per_page, order_by=(), exclude_deleted=True, **conditions) -> (items, total)`
  - `first(table, exclude_deleted=True, **conditions) -> Optional[DynamicModel]`
  - `last(table, exclude_deleted=True, **conditions) -> Optional[DynamicModel]`
  - `get_by(table, exclude_deleted=True, **conditions) -> Optional[DynamicModel]`
  - `exists_by_id(table, row_id, exclude_deleted=True) -> bool`

- Aggregates:
  - `count(table, exclude_deleted=True, **conditions) -> int`
  - `exists(table, exclude_deleted=True, **conditions) -> bool`
  - `aggregate(table, func, column, exclude_deleted=True, **conditions) -> Any`
    - func Beispiele: "SUM", "MIN", "MAX", "AVG", "COUNT", "COUNT(DISTINCT)"

- Update/Delete (bedingungenbasiert):
  - `update_by_conditions(table, updates: Dict, **conditions) -> int`
  - `delete_by_conditions(table, **conditions) -> int`

- Bulk Update / Batch:
  - `bulk_update(table, rows: List[Dict], key="id", update_cols=None) -> int`
  - `execute_batch(query: str, params: List[Sequence], page_size=100) -> int`

- Order By:
  - `order_by` akzeptiert Iterable von Spaltennamen. Vorangestelltes `-` = DESC (z. B. `("-id", "name")`).

- Conditions:
  - Standardmäßig Gleichheitsabfragen.
  - Bei `list/tuple/set` wird `col = ANY(%s)` (Arrayvergleich) genutzt.
  - Für komplexere Bedingungen nutze `raw_query()`.



## Instanzen & Attribute

Instanzen spiegeln die DB‑Zeile und ermöglichen dynamisches Hinzufügen von Spalten.

- Initialisierung: `obj = DynamicModel("table", id)`
- Attribute lesen: `obj.name`
- Attribute setzen:
  - Existierende Spalte: direktes Update in DB.
  - Neue Spalte: Spalte wird (mit Typ‑Inferenz) per ALTER TABLE angelegt und anschließend befüllt.
- `save()`: schreibt alle Daten aus `obj._data` (außer id) in die DB.
- `save_with_version(version_col="version") -> bool`:
  - Optimistic Locking. Nutzt WHERE `version = current_version`. Vorher `ensure_version_column()` aufrufen.
- `delete()`: physisches Löschen.
- `to_dict() -> Dict[str, Any]`: Kopie der aktuellen Werte.
- `refresh()`: lädt Daten erneut aus der DB.
- `clone_row(overrides=None) -> DynamicModel`: Kopiert Zeile in dieselbe Tabelle (ohne id).
- `copy_row_to_table(target_table, overrides=None) -> DynamicModel`: Kopiert Zeile in andere Tabelle.

Beispiel:
```python
u = DM.create("users", email="a@b.c", meta={"lang": "de"})  # meta -> JSONB
u.role = "admin"  # erzeugt Spalte role (TEXT) falls nicht existiert
u.save()
```



## Soft‑Delete

- `soft_delete()`: setzt `deleted_at = NOW()` und `deleted = TRUE` (legt Spalten bei Bedarf an).
- `restore_soft_deleted()`: setzt `deleted_at = NULL`, `deleted = FALSE`.
- `purge_soft_deleted_older_than(table, minutes) -> int`: löscht endgültig.

Auto‑Filter:
- `find_ids`, `get_all`, `paginate`, `count`, `exists` filtern standardmäßig `deleted = FALSE`, falls Spalte existiert.
- Deaktivierbar mit `exclude_deleted=False`.

Hinweis: `soft_delete()` nutzt `datetime.utcnow()` (naiv) für TIMESTAMP. Für Zeitzonen nutze TIMESTAMPTZ‑Spalten.



## Relationen

- `children(child_table, fk_column) -> List[DynamicModel]`
- `has_many(child_table, fk_column) -> List[DynamicModel]` (Alias)
- `has_one(child_table, fk_column) -> Optional[DynamicModel]`
- `belongs_to(parent_table, fk_column="") -> Optional[DynamicModel]`
  - Bei leerem `fk_column` wird `<parent_table>_id` erwartet.

Beispiel:
```python
user = DM.first("users")
orders = user.has_many("orders", fk_column="user_id")
account = user.belongs_to("accounts")  # erwartet accounts_id
```



## Migrationen

- `add_migration(name, fn)`: registriert Migration.
- `run_migrations()`: führt Migrationen in Transaktionen aus und protokolliert sie in `schema_migrations(name, applied_at)`.

Beispiel:
```python
def m_001_add_index():
    DM.add_index("users", "email", unique=True)

DM.add_migration("001_add_users_email_index", m_001_add_index)
DM.run_migrations()
```



## Audit‑Trail

- `enable_audit_trail(table, audit_table="audit_log")`
  - Legt `audit_log` an (falls fehlend) und einen Trigger, der bei INSERT/UPDATE/DELETE jeweils JSONB‑Snapshots schreibt.

Beispiel:
```python
DM.enable_audit_trail("users")
```



## Raw SQL, Streaming, Explain

- `raw_query(query, params=()) -> List[Dict[str, Any]]`:
  - Gibt Ergebnis als Liste von Dicts zurück; bei Befehlen ohne Resultset: `[]`.
- `stream_query(query, params=(), fetch_size=1000) -> Iterator[Dict[str,Any]]`:
  - Serverseitiger Cursor (speicherschonend). Verbindung bleibt bis Ende des Iterierens belegt.
- `explain(query, params=(), analyze=True) -> str`:
  - Liefert EXPLAIN (ANALYZE, BUFFERS)‑Plan als String.

Beispiel:
```python
for row in DM.stream_query("SELECT * FROM big_table ORDER BY id", fetch_size=5000):
    process(row)

print(DM.explain("SELECT * FROM users WHERE email = %s", ["a@b.c"]))
```



## Utilities

- `vacuum_analyze(table=None)`: führt VACUUM ANALYZE aus (optional für eine Tabelle).
- `healthcheck()`: siehe oben.



## Hooks

- `register_before_insert(table_or_star, fn)`
- `register_after_insert(table_or_star, fn)`

Verwendung:
```python
def before_any_insert(data: dict):import contextlib
import datetime
from typing import List, Dict, Any, Callable, Iterable, Tuple, Optional

import psycopg2
from psycopg2 import sql
import psycopg2.extras
from psycopg2.pool import SimpleConnectionPool


class DynamicModel:
    """
    Mini-ORM mit umfangreichen CRUD-, DDL- und Utility-Methoden,
    Connection-Pooling, Hooks, Migrationen, Soft-Delete, u.v.m.
    """

    # --- Klassenattribute für Connection/Pool, Hooks und Migrationen ---
    _connection: Optional[psycopg2.extensions.connection] = None
    _pool: Optional[SimpleConnectionPool] = None
    _before_hooks: Dict[str, List[Callable[[dict], None]]] = {}
    _after_hooks: Dict[str, List[Callable[[dict], None]]] = {}
    _migrations: List[Tuple[str, Callable[[], None]]] = []

    # ---------------------- Verbindungs-Setup ----------------------------

    @classmethod
    def connect(cls, **db_params):
        """
        Einfache Einzelverbindung (kein Pool).
        """
        cls._connection = psycopg2.connect(**db_params)
        cls._connection.autocommit = False
        cls._pool = None

    @classmethod
    def connect_pool(cls, minconn: int = 1, maxconn: int = 5, **db_params):
        """
        Verbindungs-Pooling via psycopg2.pool.SimpleConnectionPool.
        """
        cls._pool = SimpleConnectionPool(minconn, maxconn, **db_params)
        cls._connection = None

    @classmethod
    @contextlib.contextmanager
    def _get_cursor(cls, dict_cursor: bool = False):
        """
        Interner Context-Manager, der je nach Setup
        entweder aus dem Pool oder der Einzelverbindung greift.
        """
        if cls._connection is None and cls._pool is None:
            raise RuntimeError("Bitte erst DynamicModel.connect() oder connect_pool() aufrufen.")

        conn = cls._pool.getconn() if cls._pool else cls._connection
        cur_cls = psycopg2.extras.RealDictCursor if dict_cursor else None
        cur = conn.cursor(cursor_factory=cur_cls)
        try:
            yield conn, cur
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            if cls._pool:
                cls._pool.putconn(conn)

    # ----------------------- Hook-System ---------------------------------

    @classmethod
    def register_before_insert(cls, table: str, fn: Callable[[dict], None]):
        cls._before_hooks.setdefault(table, []).append(fn)

    @classmethod
    def register_after_insert(cls, table: str, fn: Callable[[dict], None]):
        cls._after_hooks.setdefault(table, []).append(fn)

    @classmethod
    def _run_before_hooks(cls, table: str, row: dict):
        for fn in cls._before_hooks.get(table, []):
            fn(row)

    @classmethod
    def _run_after_hooks(cls, table: str, row: dict):
        for fn in cls._after_hooks.get(table, []):
            fn(row)

    # -------------------- Schema-Inspektion ------------------------------

    @classmethod
    def inspect_schema(cls, table: str) -> List[Dict[str, Any]]:
        """
        Gibt Metadaten zu Spalten zurück: name, type, nullable, default…
        """
        qry = """
            SELECT column_name, data_type, is_nullable, column_default
              FROM information_schema.columns
             WHERE table_schema = 'public' AND table_name = %s
             ORDER BY ordinal_position
        """
        with cls._get_cursor(dict_cursor=True) as (_, cur):
            cur.execute(qry, (table,))
            return cur.fetchall()

    # -------------------- Tabellen-Definition ---------------------------

    @classmethod
    def create_table(cls, table: str, schema: Dict[str, str]):
        """
        CREATE TABLE IF NOT EXISTS … plus automatisch 'id SERIAL PRIMARY KEY'.
        schema: dict of column -> SQL-Type.
        """
        cols = [sql.SQL("id SERIAL PRIMARY KEY")]
        for col, typ in schema.items():
            cols.append(
                sql.SQL("{} {}").format(sql.Identifier(col), sql.SQL(typ))
            )
        q = sql.SQL("CREATE TABLE IF NOT EXISTS {} ({})").format(
            sql.Identifier(table),
            sql.SQL(", ").join(cols)
        )
        with cls._get_cursor() as (_, cur):
            cur.execute(q)

    # -------------------- Einzel-INSERT mit Hooks ------------------------

    @classmethod
    def create(cls, table: str, **kwargs) -> "DynamicModel":
        """
        Legt einen neuen Datensatz an. Fehlende Spalten werden als TEXT ergänzt.
        Führt Hooks BEFORE/AFTER Insert aus.
        """
        # 1) existierende Spalten ermitteln
        infos = cls.inspect_schema(table)
        if not infos:
            raise ValueError(f"Tabelle '{table}' existiert nicht.")
        existing = {r["column_name"] for r in infos}

        # 2) Missing columns anlegen
        for col in kwargs:
            if col not in existing:
                with cls._get_cursor() as (_, cur):
                    cur.execute(
                        sql.SQL("ALTER TABLE {} ADD COLUMN {} TEXT").format(
                            sql.Identifier(table), sql.Identifier(col)
                        )
                    )

        # 3) Hooks BEFORE
        cls._run_before_hooks(table, kwargs)

        # 4) INSERT bauen
        cols = [sql.Identifier(c) for c in kwargs]
        phs = [sql.Placeholder()] * len(kwargs)
        ins = sql.SQL("INSERT INTO {} ({}) VALUES ({}) RETURNING id").format(
            sql.Identifier(table),
            sql.SQL(", ").join(cols),
            sql.SQL(", ").join(phs)
        )
        vals = list(kwargs.values())
        with cls._get_cursor() as (_, cur):
            cur.execute(ins, vals)
            new_id = cur.fetchone()[0]

        # 5) Hooks AFTER
        cls._run_after_hooks(table, kwargs)

        return cls(table, new_id)

    # -------------------- Bulk-INSERT (execute_values) -------------------

    @classmethod
    def bulk_create(cls, table: str, rows: List[Dict[str, Any]]) -> List[int]:
        """
        Fügt mehrere Datensätze in einem großen INSERT ein.
        Fehlende Spalten werden als TEXT ergänzt.
        """
        if not rows:
            return []
        all_cols = set().union(*(r.keys() for r in rows))
        existing = {r["column_name"] for r in cls.inspect_schema(table)}
        missing = all_cols - existing
        # fehlende Spalten anlegen
        for col in missing:
            with cls._get_cursor() as (_, cur):
                cur.execute(
                    sql.SQL("ALTER TABLE {} ADD COLUMN {} TEXT").format(
                        sql.Identifier(table), sql.Identifier(col)
                    )
                )
        ordered = sorted(all_cols)
        cols_sql = sql.SQL(",").join(map(sql.Identifier, ordered))
        values = [[row.get(c) for c in ordered] for row in rows]
        # BEFORE-Hooks
        for row in rows:
            cls._run_before_hooks(table, row)
        ins = sql.SQL("INSERT INTO {} ({}) VALUES %s RETURNING id").format(
            sql.Identifier(table), cols_sql
        )
        with cls._get_cursor() as (_, cur):
            psycopg2.extras.execute_values(cur, ins, values)
            new_ids = [r[0] for r in cur.fetchall()]
        # AFTER-Hooks
        for row in rows:
            cls._run_after_hooks(table, row)
        return new_ids

    # -------------------- Hilfsfunktionen für WHERE -----------------------

    @classmethod
    def _build_conditions(cls, conditions: Dict[str, Any]) -> Tuple[sql.SQL, List[Any]]:
        if not conditions:
            return sql.SQL(""), []
        parts, vals = [], []
        for col, val in conditions.items():
            parts.append(sql.SQL("{} = %s").format(sql.Identifier(col)))
            vals.append(val)
        return sql.SQL(" AND ").join(parts), vals

    # -------------------- find_ids mit ORDER-BY ---------------------------

    @classmethod
    def find_ids(cls,
                 table: str,
                 order_by: Iterable[str] = (),
                 **conditions) -> List[int]:
        """
        SELECT id FROM … WHERE … ORDER BY …
        """
        cond_sql, cond_vals = cls._build_conditions(conditions)
        q = sql.SQL("SELECT id FROM {}").format(sql.Identifier(table))
        if cond_sql:
            q += sql.SQL(" WHERE {}").format(cond_sql)
        if order_by:
            ob_parts = []
            for c in order_by:
                direction = sql.SQL("DESC") if c.startswimport contextlib
import datetime
from typing import List, Dict, Any, Callable, Iterable, Tuple, Optional

import psycopg2
from psycopg2 import sql
import psycopg2.extras
from psycopg2.pool import SimpleConnectionPool


class DynamicModel:
    """
    Mini-ORM mit umfangreichen CRUD-, DDL- und Utility-Methoden,
    Connection-Pooling, Hooks, Migrationen, Soft-Delete, u.v.m.
    """

    # --- Klassenattribute für Connection/Pool, Hooks und Migrationen ---
    _connection: Optional[psycopg2.extensions.connection] = None
    _pool: Optional[SimpleConnectionPool] = None
    _before_hooks: Dict[str, List[Callable[[dict], None]]] = {}
    _after_hooks: Dict[str, List[Callable[[dict], None]]] = {}
    _migrations: List[Tuple[str, Callable[[], None]]] = []

    # ---------------------- Verbindungs-Setup ----------------------------

    @classmethod
    def connect(cls, **db_params):
        """
        Einfache Einzelverbindung (kein Pool).
        """
        cls._connection = psycopg2.connect(**db_params)
        cls._connection.autocommit = False
        cls._pool = None

    @classmethod
    def connect_pool(cls, minconn: int = 1, maxconn: int = 5, **db_params):
        """
        Verbindungs-Pooling via psycopg2.pool.SimpleConnectionPool.
        """
        cls._pool = SimpleConnectionPool(minconn, maxconn, **db_params)
        cls._connection = None

    @classmethod
    @contextlib.contextmanager
    def _get_cursor(cls, dict_cursor: bool = False):
        """
        Interner Context-Manager, der je nach Setup
        entweder aus dem Pool oder der Einzelverbindung greift.
        """
        if cls._connection is None and cls._pool is None:
            raise RuntimeError("Bitte erst DynamicModel.connect() oder connect_pool() aufrufen.")

        conn = cls._pool.getconn() if cls._pool else cls._connection
        cur_cls = psycopg2.extras.RealDictCursor if dict_cursor else None
        cur = conn.cursor(cursor_factory=cur_cls)
        try:
            yield conn, cur
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            if cls._pool:
                cls._pool.putconn(conn)

    # ----------------------- Hook-System ---------------------------------

    @classmethod
    def register_before_insert(cls, table: str, fn: Callable[[dict], None]):
        cls._before_hooks.setdefault(table, []).append(fn)

    @classmethod
    def register_after_insert(cls, table: str, fn: Callable[[dict], None]):
        cls._after_hooks.setdefault(table, []).append(fn)

    @classmethod
    def _run_before_hooks(cls, table: str, row: dict):
        for fn in cls._before_hooks.get(table, []):
            fn(row)

    @classmethod
    def _run_after_hooks(cls, table: str, row: dict):
        for fn in cls._after_hooks.get(table, []):
            fn(row)

    # -------------------- Schema-Inspektion ------------------------------

    @classmethod
    def inspect_schema(cls, table: str) -> List[Dict[str, Any]]:
        """
        Gibt Metadaten zu Spalten zurück: name, type, nullable, default…
        """
        qry = """
            SELECT column_name, data_type, is_nullable, column_default
              FROM information_schema.columns
             WHERE table_schema = 'public' AND table_name = %s
             ORDER BY ordinal_position
        """
        with cls._get_cursor(dict_cursor=True) as (_, cur):
            cur.execute(qry, (table,))
            return cur.fetchall()

    # -------------------- Tabellen-Definition ---------------------------

    @classmethod
    def create_table(cls, table: str, schema: Dict[str, str]):
        """
        CREATE TABLE IF NOT EXISTS … plus automatisch 'id SERIAL PRIMARY KEY'.
        schema: dict of column -> SQL-Type.
        """
        cols = [sql.SQL("id SERIAL PRIMARY KEY")]
        for col, typ in schema.items():
            cols.append(
                sql.SQL("{} {}").format(sql.Identifier(col), sql.SQL(typ))
            )
        q = sql.SQL("CREATE TABLE IF NOT EXISTS {} ({})").format(
            sql.Identifier(table),
            sql.SQL(", ").join(cols)
        )
        with cls._get_cursor() as (_, cur):
            cur.execute(q)

    # -------------------- Einzel-INSERT mit Hooks ------------------------

    @classmethod
    def create(cls, table: str, **kwargs) -> "DynamicModel":
        """
        Legt einen neuen Datensatz an. Fehlende Spalten werden als TEXT ergänzt.
        Führt Hooks BEFORE/AFTER Insert aus.
        """
        # 1) existierende Spalten ermitteln
        infos = cls.inspect_schema(table)
        if not infos:
            raise ValueError(f"Tabelle '{table}' existiert nicht.")
        existing = {r["column_name"] for r in infos}

        # 2) Missing columns anlegen
        for col in kwargs:
            if col not in existing:
                with cls._get_cursor() as (_, cur):
                    cur.execute(
                        sql.SQL("ALTER TABLE {} ADD COLUMN {} TEXT").format(
                            sql.Identifier(table), sql.Identifier(col)
                        )
                    )

        # 3) Hooks BEFORE
        cls._run_before_hooks(table, kwargs)

        # 4) INSERT bauen
        cols = [sql.Identifier(c) for c in kwargs]
        phs = [sql.Placeholder()] * len(kwargs)
        ins = sql.SQL("INSERT INTO {} ({}) VALUES ({}) RETURNING id").format(
            sql.Identifier(table),
            sql.SQL(", ").join(cols),
            sql.SQL(", ").join(phs)
        )
        vals = list(kwargs.values())
        with cls._get_cursor() as (_, cur):
            cur.execute(ins, vals)
            new_id = cur.fetchone()[0]

        # 5) Hooks AFTER
        cls._run_after_hooks(table, kwargs)

        return cls(table, new_id)

    # -------------------- Bulk-INSERT (execute_values) -------------------

    @classmethod
    def bulk_create(cls, table: str, rows: List[Dict[str, Any]]) -> List[int]:
        """
        Fügt mehrere Datensätze in einem großen INSERT ein.
        Fehlende Spalten werden als TEXT ergänzt.
        """
        if not rows:
            return []
        all_cols = set().union(*(r.keys() for r in rows))
        existing = {r["column_name"] for r in cls.inspect_schema(table)}
        missing = all_cols - existing
        # fehlende Spalten anlegen
        for col in missing:
            with cls._get_cursor() as (_, cur):
                cur.execute(
                    sql.SQL("ALTER TABLE {} ADD COLUMN {} TEXT").format(
                        sql.Identifier(table), sql.Identifier(col)
                    )
                )
        ordered = sorted(all_cols)
        cols_sql = sql.SQL(",").join(map(sql.Identifier, ordered))
        values = [[row.get(c) for c in ordered] for row in rows]
        # BEFORE-Hooks
        for row in rows:
            cls._run_before_hooks(table, row)
        ins = sql.SQL("INSERT INTO {} ({}) VALUES %s RETURNING id").format(
            sql.Identifier(table), cols_sql
        )
        with cls._get_cursor() as (_, cur):
            psycopg2.extras.execute_values(cur, ins, values)
            new_ids = [r[0] for r in cur.fetchall()]
        # AFTER-Hooks
        for row in rows:
            cls._run_after_hooks(table, row)
        return new_ids

    # -------------------- Hilfsfunktionen für WHERE -----------------------

    @classmethod
    def _build_conditions(cls, conditions: Dict[str, Any]) -> Tuple[sql.SQL, List[Any]]:
        if not conditions:
            return sql.SQL(""), []
        parts, vals = [], []
        for col, val in conditions.items():
            parts.append(sql.SQL("{} = %s").format(sql.Identifier(col)))
            vals.append(val)
        return sql.SQL(" AND ").join(parts), vals

    # -------------------- find_ids mit ORDER-BY ---------------------------

    @classmethod
    def find_ids(cls,
                 table: str,
                 order_by: Iterable[str] = (),
                 **conditions) -> List[int]:
        """
        SELECT id FROM … WHERE … ORDER BY …
        """
        cond_sql, cond_vals = cls._build_conditions(conditions)
        q = sql.SQL("SELECT id FROM {}").format(sql.Identifier(table))
        if cond_sql:
            q += sql.SQL(" WHERE {}").format(cond_sql)
        if order_by:
            ob_parts = []
            for c in order_by:
                direction = sql.SQL("DESC") if c.startswith("-") else sql.SQL("ASC")
                ident = sql.Identifier(c.lstrip("-"))
                ob_parts.append(ident + sql.SQL(" ") + direction)
            q += sql.SQL(" ORDER BY ") + sql.SQL(", ").join(ob_parts)
        with cls._get_cursor() as (_, cur):
            cur.execute(q, cond_vals)
            return [r[0] for r in cur.fetchall()]

    # -------------------- List all IDs ---------------------------

    @classmethod
    def list_all_ids(cls, table: str) -> List[int]:
        q = sql.SQL("SELECT id FROM {}").format(sql.Identifier(table))
        with cls._get_cursor() as (_, cur):
            cur.execute(q)
            return [r[0] for r in cur.fetchall()]

    # -------------------- Update/Delete by Conditions ---------------------

    @classmethod
    def update_by_conditions(cls,
                             table: str,
                             updates: Dict[str, Any],
                             **conditions) -> int:
        """
        UPDATE … SET … WHERE …; gibt rowcount zurück.
        """
        if not updates:
            return 0
        set_parts, set_vals = [], []
        for col, val in updates.items():
            set_parts.append(sql.SQL("{} = %s").format(sql.Identifier(col)))
            set_vals.append(val)
        cond_sql, cond_vals = cls._build_conditions(conditions)
        q = sql.SQL("UPDATE {} SET {}").format(
            sql.Identifier(table),
            sql.SQL(", ").join(set_parts)
        )
        if cond_sql:
            q += sql.SQL(" WHERE {}").format(cond_sql)
        with cls._get_cursor() as (_, cur):
            cur.execute(q, set_vals + cond_vals)
            return cur.rowcount

    @classmethod
    def delete_by_conditions(cls, table: str, **conditions) -> int:
        """
        DELETE FROM … WHERE …; gibt rowcount zurück.
        """
        cond_sql, cond_vals = cls._build_conditions(conditions)
        q = sql.SQL("DELETE FROM {}").format(sql.Identifier(table))
        if cond_sql:
            q += sql.SQL(" WHERE {}").format(cond_sql)
        with cls._get_cursor() as (_, cur):
            cur.execute(q, cond_vals)
            return cur.rowcount

    # -------------------- Count / Exists -------------------------------

    @classmethod
    def count(cls, table: str, **conditions) -> int:
        cond_sql, cond_vals = cls._build_conditions(conditions)
        q = sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(table))
        if cond_sql:
            q += sql.SQL(" WHERE {}").format(cond_sql)
        with cls._get_cursor() as (_, cur):
            cur.execute(q, cond_vals)
            return cur.fetchone()[0]

    @classmethod
    def exists(cls, table: str, **conditions) -> bool:
        return cls.count(table, **conditions) > 0

    # -------------------- get_all / paginate ----------------------------

    @classmethod
    def get_all(cls,
                table: str,
                order_by: Iterable[str] = (),
                **conditions) -> List["DynamicModel"]:
        ids = cls.find_ids(table, order_by=order_by, **conditions)
        return [cls(table, i) for i in ids]

    @classmethod
    def paginate(cls,
                 table: str,
                 page: int = 1,
                 per_page: int = 25,
                 order_by: Iterable[str] = (),
                 **conditions) -> List["DynamicModel"]:
        if page < 1:
            page = 1
        offset = (page - 1) * per_page
        cond_sql, cond_vals = cls._build_conditions(conditions)
        q = sql.SQL("SELECT id FROM {}").format(sql.Identifier(table))
        if cond_sql:
            q += sql.SQL(" WHERE {}").format(cond_sql)
        if order_by:
            ob_parts = []
            for c in order_by:
                direction = sql.SQL("DESC") if c.startswith("-") else sql.SQL("ASC")
                ident = sql.Identifier(c.lstrip("-"))
                ob_parts.append(ident + sql.SQL(" ") + direction)
            q += sql.SQL(" ORDER BY ") + sql.SQL(", ").join(ob_parts)
        q += sql.SQL(" LIMIT %s OFFSET %s")
        with cls._get_cursor() as (_, cur):
            cur.execute(q, cond_vals + [per_page, offset])
            ids = [r[0] for r in cur.fetchall()]
        return [cls(table, i) for i in ids]

    # -------------------- DDL-Operationen -------------------------------

    @classmethod
    def add_index(cls, table: str, column: str, unique: bool = False):
        idx_name = f"{table}_{column}_{'uniq' if unique else 'idx'}"
        stmt = sql.SQL("CREATE {uniq} INDEX IF NOT EXISTS {iname} ON {t} ({c})").format(
            uniq=sql.SQL("UNIQUE") if unique else sql.SQL(""),
            iname=sql.Identifier(idx_name),
            t=sql.Identifier(table),
            c=sql.Identifier(column)
        )
        with cls._get_cursor() as (_, cur):
            cur.execute(stmt)

    @classmethod
    def drop_column(cls, table: str, column: str):
        stmt = sql.SQL("ALTER TABLE {t} DROP COLUMN IF EXISTS {c}").format(
            t=sql.Identifier(table),
            c=sql.Identifier(column)
        )
        with cls._get_cursor() as (_, cur):
            cur.execute(stmt)

    @classmethod
    def rename_column(cls, table: str, old: str, new: str):
        stmt = sql.SQL("ALTER TABLE {t} RENAME COLUMN {o} TO {n}").format(
            t=sql.Identifier(table),
            o=sql.Identifier(old),
            n=sql.Identifier(new)
        )
        with cls._get_cursor() as (_, cur):
            cur.execute(stmt)

    # -------------------- Soft Delete -----------------------------------

    def soft_delete(self):
        """
        Setzt ein 'deleted_at' Timestamp-Feld auf NOW() und 'deleted' auf True, statt physisch zu löschen.
        """
        # deleted_at-Feld anlegen falls nicht vorhanden
        if "deleted_at" not in self._columns:
            with self._get_cursor() as (_, cur):
                cur.execute(
                    sql.SQL(
                        "ALTER TABLE {t} ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMP"
                    ).format(t=sql.Identifier(self._table))
                )
            self._columns.add("deleted_at")
        # deleted-Feld anlegen falls nicht vorhanden
        if "deleted" not in self._columns:
            with self._get_cursor() as (_, cur):
                cur.execute(
                    sql.SQL(
                        "ALTER TABLE {t} ADD COLUMN IF NOT EXISTS deleted BOOLEAN DEFAULT FALSE"
                    ).format(t=sql.Identifier(self._table))
                )
            self._columns.add("deleted")

        now = datetime.datetime.utcnow()
        stmt = sql.SQL("UPDATE {t} SET deleted_at = %s, deleted = %s WHERE id = %s").format(
            t=sql.Identifier(self._table)
        )
        with self._get_cursor() as (_, cur):
            cur.execute(stmt, (now, True, self._id))
        self._data["deleted_at"] = now
        self._data["deleted"] = True

    def restore_soft_deleted(self):
        """
        Setzt 'deleted_at' auf NULL und 'deleted' auf False, um einen Datensatz wiederherzustellen.
        """
        if "deleted_at" not in self._columns or "deleted" not in self._columns:
            # Nichts zu tun, wenn Felder fehlen
            return
        stmt = sql.SQL("UPDATE {t} SET deleted_at = NULL, deleted = %s WHERE id = %s").format(
            t=sql.Identifier(self._table)
        )
        with self._get_cursor() as (_, cur):
            cur.execute(stmt, (False, self._id))
        self._data["deleted_at"] = None
        self._data["deleted"] = False

    @classmethod
    def purge_soft_deleted_older_than(cls, table: str, minutes: int) -> int:
        """
        Löscht alle Datensätze mit deleted=True und deleted_at älter als X Minuten endgültig.
        Gibt die Anzahl der gelöschten Zeilen zurück.
        """
        # deleted/deleted_at-Felder anlegen falls nicht vorhanden
        infos = cls.inspect_schema(table)
        columns = {r["column_name"] for r in infos}
        if "deleted" not in columns:
            with cls._get_cursor() as (_, cur):
                cur.execute(
                    sql.SQL(
                        "ALTER TABLE {t} ADD COLUMN IF NOT EXISTS deleted BOOLEAN DEFAULT FALSE"
                    ).format(t=sql.Identifier(table))
                )
        if "deleted_at" not in columns:
            with cls._get_cursor() as (_, cur):
                cur.execute(
                    sql.SQL(
                        "ALTER TABLE {t} ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMP"
                    ).format(t=sql.Identifier(table))
                )
        stmt = sql.SQL(
            "DELETE FROM {t} WHERE deleted = TRUE AND deleted_at IS NOT NULL AND deleted_at < (NOW() - INTERVAL %s MINUTE)"
        ).format(t=sql.Identifier(table))
        with cls._get_cursor() as (_, cur):
            cur.execute(stmt, (minutes,))
            return cur.rowcount

    # TODO: 
    # - Optional: Soft-Delete für mehrere Tabellen gleichzeitig ermöglichen
    # - Optional: Soft-Delete-Status in Queries automatisch berücksichtigen (z.B. nur nicht-gelöschte laden)
    # - Optional: Automatische Zeitsteuerung für purge (z.B. per Scheduler)
    # ...existing code...

    # -------------------- Transaction-Context --------------------------

    @classmethod
    @contextlib.contextmanager
    def transaction(cls):
        """
        with DynamicModel.transaction():
            … mehrere Operationen …
        commit/rollback automatisch
        """
        if cls._connection is None and cls._pool is None:
            raise RuntimeError("Keine Datenbankverbindung.")
        conn = cls._pool.getconn() if cls._pool else cls._connection
        try:
            yield
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            if cls._pool:
                cls._pool.putconn(conn)

    # -------------------- Raw SQL ---------------------------------------

    @classmethod
    def raw_query(cls, query: str, params: Iterable[Any] = ()) -> List[Dict[str, Any]]:
        """
        Führt beliebiges SQL aus, gibt List[Dict] zurück (oder [] ohne Ergebnis).
        """
        with cls._get_cursor(dict_cursor=True) as (_, cur):
            cur.execute(query, params)
            try:
                return cur.fetchall()
            except psycopg2.ProgrammingError:
                return []

    # -------------------- Simple Relationship --------------------------

    def children(self, child_table: str, fk_column: str) -> List["DynamicModel"]:
        """
        parent.children("orders", fk_column="user_id")
        """
        ids = DynamicModel.find_ids(child_table, **{fk_column: self._id})
        return [DynamicModel(child_table, i) for i in ids]

    # -------------------- Migration-Framework -------------------------

    @classmethod
    def add_migration(cls, name: str, fn: Callable[[], None]):
        """
        Registriert eine Migrations-Funktion, die einmalig ausgeführt wird.
        """
        cls._migrations.append((name, fn))

    @classmethod
    def run_migrations(cls):
        """
        Führt alle registrierten Migrationen in Transaktionen aus.
        """
        for name, fn in cls._migrations:
            print(f"Running migration: {name}")
            with cls.transaction():
                fn()

    # -------------------- Instanz-Logik (Rest des ORMs) ---------------

    def __init__(self, table: str, row_id: int):
        if self._connection is None and self._pool is None:
            raise RuntimeError("Bitte erst DynamicModel.connect() aufrufen.")
        self._table = table
        self._id = row_id
        self._columns: set = set()
        self._data: dict = {}
        self._load_columns()
        self._load_data()

    def _load_columns(self):
        infos = self.inspect_schema(self._table)
        if not infos:
            raise ValueError(f"Tabelle '{self._table}' existiert nicht.")
        self._columns = {r["column_name"] for r in infos}

    def _load_data(self):
        cols_sql = sql.SQL(", ").join(map(sql.Identifier, self._columns))
        q = sql.SQL("SELECT {cols} FROM {t} WHERE id = %s").format(
            cols=cols_sql,
            t=sql.Identifier(self._table)
        )
        with self._get_cursor(dict_cursor=True) as (_, cur):
            cur.execute(q, (self._id,))
            row = cur.fetchone()
        if not row:
            raise ValueError(f"Kein Datensatz mit id={self._id} in Tabelle {self._table}.")
        self._data = dict(row)

    def __getattr__(self, name: str) -> Any:
        if name in self._columns:
            return self._data.get(name)
        raise AttributeError(f"'{type(self).__name__}' hat kein Attribut '{name}'")

    def __setattr__(self, name: str, value: Any):
        # interne Felder weiterreichen
        if name in ("_table", "_id", "_columns", "_data",
                    "_connection", "_pool",
                    "_before_hooks", "_after_hooks", "_migrations"):
            return super().__setattr__(name, value)

        # Update einer existierenden Spalte
        if name in self._columns:
            stmt = sql.SQL("UPDATE {t} SET {c} = %s WHERE id = %s").format(
                t=sql.Identifier(self._table),
                c=sql.Identifier(name)
            )
            with self._get_cursor() as (_, cur):
                cur.execute(stmt, (value, self._id))
            self._data[name] = value
            return

        # Neue Spalte hinzufügen
        with self._get_cursor() as (_, cur):
            cur.execute(
                sql.SQL("ALTER TABLE {t} ADD COLUMN {c} TEXT").format(
                    t=sql.Identifier(self._table),
                    c=sql.Identifier(name)
                )
            )
        self._columns.add(name)
        # Dann befüllen
        stmt = sql.SQL("UPDATE {t} SET {c} = %s WHERE id = %s").format(
            t=sql.Identifier(self._table),
            c=sql.Identifier(name)
        )
        with self._get_cursor() as (_, cur):
            cur.execute(stmt, (value, self._id))
        self._data[name] = value

    def save(self):
        """
        Speichert alle im _data gehaltenen Werte (außer id).
        """
        cols = [c for c in self._columns if c != "id"]
        if not cols:
            return
        parts = [
            sql.SQL("{} = %s").format(sql.Identifier(c))
            for c in cols
        ]
        stmt = sql.SQL("UPDATE {t} SET {sets} WHERE id = %s").format(
            t=sql.Identifier(self._table),
            sets=sql.SQL(", ").join(parts)
        )
        vals = [self._data[c] for c in cols] + [self._id]
        with self._get_cursor() as (_, cur):
            cur.execute(stmt, vals)

    def delete(self):
        """
        Physisches Löschen dieses Datensatzes.
        """
        stmt = sql.SQL("DELETE FROM {t} WHERE id = %s").format(
            t=sql.Identifier(self._table)
        )
        with self._get_cursor() as (_, cur):
            cur.execute(stmt, (self._id,))ith("-") else sql.SQL("ASC")
                ident = sql.Identifier(c.lstrip("-"))
                ob_parts.append(ident + sql.SQL(" ") + direction)
            q += sql.SQL(" ORDER BY ") + sql.SQL(", ").join(ob_parts)
        with cls._get_cursor() as (_, cur):
            cur.execute(q, cond_vals)
            return [r[0] for r in cur.fetchall()]

    # -------------------- List all IDs ---------------------------
import contextlib
import datetime
from typing import List, Dict, Any, Callable, Iterable, Tuple, Optional

import psycopg2
from psycopg2 import sql
import psycopg2.extras
from psycopg2.pool import SimpleConnectionPool


class DynamicModel:
    """
    Mini-ORM mit umfangreichen CRUD-, DDL- und Utility-Methoden,
    Connection-Pooling, Hooks, Migrationen, Soft-Delete, u.v.m.
    """

    # --- Klassenattribute für Connection/Pool, Hooks und Migrationen ---
    _connection: Optional[psycopg2.extensions.connection] = None
    _pool: Optional[SimpleConnectionPool] = None
    _before_hooks: Dict[str, List[Callable[[dict], None]]] = {}
    _after_hooks: Dict[str, List[Callable[[dict], None]]] = {}
    _migrations: List[Tuple[str, Callable[[], None]]] = []

    # ---------------------- Verbindungs-Setup ----------------------------

    @classmethod
    def connect(cls, **db_params):
        """
        Einfache Einzelverbindung (kein Pool).
        """
        cls._connection = psycopg2.connect(**db_params)
        cls._connection.autocommit = False
        cls._pool = None

    @classmethod
    def connect_pool(cls, minconn: int = 1, maxconn: int = 5, **db_params):
        """
        Verbindungs-Pooling via psycopg2.pool.SimpleConnectionPool.
        """
        cls._pool = SimpleConnectionPool(minconn, maxconn, **db_params)
        cls._connection = None

    @classmethod
    @contextlib.contextmanager
    def _get_cursor(cls, dict_cursor: bool = False):
        """
        Interner Context-Manager, der je nach Setup
        entweder aus dem Pool oder der Einzelverbindung greift.
        """
        if cls._connection is None and cls._pool is None:
            raise RuntimeError("Bitte erst DynamicModel.connect() oder connect_pool() aufrufen.")

        conn = cls._pool.getconn() if cls._pool else cls._connection
        cur_cls = psycopg2.extras.RealDictCursor if dict_cursor else None
        cur = conn.cursor(cursor_factory=cur_cls)
        try:
            yield conn, cur
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            if cls._pool:
                cls._pool.putconn(conn)

    # ----------------------- Hook-System ---------------------------------

    @classmethod
    def register_before_insert(cls, table: str, fn: Callable[[dict], None]):
        cls._before_hooks.setdefault(table, []).append(fn)

    @classmethod
    def register_after_insert(cls, table: str, fn: Callable[[dict], None]):
        cls._after_hooks.setdefault(table, []).append(fn)

    @classmethod
    def _run_before_hooks(cls, table: str, row: dict):
        for fn in cls._before_hooks.get(table, []):
            fn(row)

    @classmethod
    def _run_after_hooks(cls, table: str, row: dict):
        for fn in cls._after_hooks.get(table, []):
            fn(row)

    # -------------------- Schema-Inspektion ------------------------------

    @classmethod
    def inspect_schema(cls, table: str) -> List[Dict[str, Any]]:
        """
        Gibt Metadaten zu Spalten zurück: name, type, nullable, default…
        """
        qry = """
            SELECT column_name, data_type, is_nullable, column_default
              FROM information_schema.columns
             WHERE table_schema = 'public' AND table_name = %s
             ORDER BY ordinal_position
        """
        with cls._get_cursor(dict_cursor=True) as (_, cur):
            cur.execute(qry, (table,))
            return cur.fetchall()

    # -------------------- Tabellen-Definition ---------------------------

    @classmethod
    def create_table(cls, table: str, schema: Dict[str, str]):
        """
        CREATE TABLE IF NOT EXISTS … plus automatisch 'id SERIAL PRIMARY KEY'.
        schema: dict of column -> SQL-Type.
        """
        cols = [sql.SQL("id SERIAL PRIMARY KEY")]
        for col, typ in schema.items():
            cols.append(
                sql.SQL("{} {}").format(sql.Identifier(col), sql.SQL(typ))
            )
        q = sql.SQL("CREATE TABLE IF NOT EXISTS {} ({})").format(
            sql.Identifier(table),
            sql.SQL(", ").join(cols)
        )
        with cls._get_cursor() as (_, cur):
            cur.execute(q)

    # -------------------- Einzel-INSERT mit Hooks ------------------------

    @classmethod
    def create(cls, table: str, **kwargs) -> "DynamicModel":
        """
        Legt einen neuen Datensatz an. Fehlende Spalten werden als TEXT ergänzt.
        Führt Hooks BEFORE/AFTER Insert aus.
        """
        # 1) existierende Spalten ermitteln
        infos = cls.inspect_schema(table)
        if not infos:
            raise ValueError(f"Tabelle '{table}' existiert nicht.")
        existing = {r["column_name"] for r in infos}

        # 2) Missing columns anlegen
        for col in kwargs:
            if col not in existing:
                with cls._get_cursor() as (_, cur):
                    cur.execute(
                        sql.SQL("ALTER TABLE {} ADD COLUMN {} TEXT").format(
                            sql.Identifier(table), sql.Identifier(col)
                        )
                    )

        # 3) Hooks BEFORE
        cls._run_before_hooks(table, kwargs)

        # 4) INSERT bauen
        cols = [sql.Identifier(c) for c in kwargs]
        phs = [sql.Placeholder()] * len(kwargs)
        ins = sql.SQL("INSERT INTO {} ({}) VALUES ({}) RETURNING id").format(
            sql.Identifier(table),
            sql.SQL(", ").join(cols),
            sql.SQL(", ").join(phs)
        )
        vals = list(kwargs.values())
        with cls._get_cursor() as (_, cur):
            cur.execute(ins, vals)
            new_id = cur.fetchone()[0]

        # 5) Hooks AFTER
        cls._run_after_hooks(table, kwargs)

        return cls(table, new_id)

    # -------------------- Bulk-INSERT (execute_values) -------------------

    @classmethod
    def bulk_create(cls, table: str, rows: List[Dict[str, Any]]) -> List[int]:
        """
        Fügt mehrere Datensätze in einem großen INSERT ein.
        Fehlende Spalten werden als TEXT ergänzt.
        """
        if not rows:
            return []
        all_cols = set().union(*(r.keys() for r in rows))
        existing = {r["column_name"] for r in cls.inspect_schema(table)}
        missing = all_cols - existing
        # fehlende Spalten anlegen
        for col in missing:
            with cls._get_cursor() as (_, cur):
                cur.execute(
                    sql.SQL("ALTER TABLE {} ADD COLUMN {} TEXT").format(
                        sql.Identifier(table), sql.Identifier(col)
                    )
                )
        ordered = sorted(all_cols)
        cols_sql = sql.SQL(",").join(map(sql.Identifier, ordered))
        values = [[row.get(c) for c in ordered] for row in rows]
        # BEFORE-Hooks
        for row in rows:
            cls._run_before_hooks(table, row)
        ins = sql.SQL("INSERT INTO {} ({}) VALUES %s RETURNING id").format(
            sql.Identifier(table), cols_sql
        )
        with cls._get_cursor() as (_, cur):
            psycopg2.extras.execute_values(cur, ins, values)
            new_ids = [r[0] for r in cur.fetchall()]
        # AFTER-Hooks
        for row in rows:
            cls._run_after_hooks(table, row)
        return new_ids

    # -------------------- Hilfsfunktionen für WHERE -----------------------

    @classmethod
    def _build_conditions(cls, conditions: Dict[str, Any]) -> Tuple[sql.SQL, List[Any]]:
        if not conditions:
            return sql.SQL(""), []
        parts, vals = [], []
        for col, val in conditions.items():
            parts.append(sql.SQL("{} = %s").format(sql.Identifier(col)))
            vals.append(val)
        return sql.SQL(" AND ").join(parts), vals

    # -------------------- find_ids mit ORDER-BY ---------------------------

    @classmethod
    def find_ids(cls,
                 table: str,
                 order_by: Iterable[str] = (),
                 **conditions) -> List[int]:
        """
        SELECT id FROM … WHERE … ORDER BY …
        """
        cond_sql, cond_vals = cls._build_conditions(conditions)
        q = sql.SQL("SELECT id FROM {}").format(sql.Identifier(table))
        if cond_sql:
            q += sql.SQL(" WHERE {}").format(cond_sql)
        if order_by:
            ob_parts = []
            for c in order_by:
                direction = sql.SQL("DESC") if c.startswith("-") else sql.SQL("ASC")
                ident = sql.Identifier(c.lstrip("-"))
                ob_parts.append(ident + sql.SQL(" ") + direction)
            q += sql.SQL(" ORDER BY ") + sql.SQL(", ").join(ob_parts)
        with cls._get_cursor() as (_, cur):
            cur.execute(q, cond_vals)
            return [r[0] for r in cur.fetchall()]

    # -------------------- List all IDs ---------------------------

    @classmethod
    def list_all_ids(cls, table: str) -> List[int]:
        q = sql.SQL("SELECT id FROM {}").format(sql.Identifier(table))
        with cls._get_cursor() as (_, cur):
            cur.execute(q)
            return [r[0] for r in cur.fetchall()]

    # -------------------- Update/Delete by Conditions ---------------------

    @classmethod
    def update_by_conditions(cls,
                             table: str,
                             updates: Dict[str, Any],
                             **conditions) -> int:
        """
        UPDATE … SET … WHERE …; gibt rowcount zurück.
        """
        if not updates:
            return 0
        set_parts, set_vals = [], []
        for col, val in updates.items():
            set_parts.append(sql.SQL("{} = %s").format(sql.Identifier(col)))
            set_vals.append(val)
        cond_sql, cond_vals = cls._build_conditions(conditions)
        q = sql.SQL("UPDATE {} SET {}").format(
            sql.Identifier(table),
            sql.SQL(", ").join(set_parts)
        )
        if cond_sql:
            q += sql.SQL(" WHERE {}").format(cond_sql)
        with cls._get_cursor() as (_, cur):
            cur.execute(q, set_vals + cond_vals)
            return cur.rowcount

    @classmethod
    def delete_by_conditions(cls, table: str, **conditions) -> int:
        """
        DELETE FROM … WHERE …; gibt rowcount zurück.
        """
        cond_sql, cond_vals = cls._build_conditions(conditions)
        q = sql.SQL("DELETE FROM {}").format(sql.Identifier(table))
        if cond_sql:
            q += sql.SQL(" WHERE {}").format(cond_sql)
        with cls._get_cursor() as (_, cur):
            cur.execute(q, cond_vals)
            return cur.rowcount

    # -------------------- Count / Exists -------------------------------

    @classmethod
    def count(cls, table: str, **conditions) -> int:
        cond_sql, cond_vals = cls._build_conditions(conditions)
        q = sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(table))
        if cond_sql:
            q += sql.SQL(" WHERE {}").format(cond_sql)
        with cls._get_cursor() as (_, cur):
            cur.execute(q, cond_vals)
            return cur.fetchone()[0]

    @classmethod
    def exists(cls, table: str, **conditions) -> bool:
        return cls.count(table, **conditions) > 0

    # -------------------- get_all / paginate ----------------------------

    @classmethod
    def get_all(cls,
                table: str,
                order_by: Iterable[str] = (),
                **conditions) -> List["DynamicModel"]:
        ids = cls.find_ids(table, order_by=order_by, **conditions)
        return [cls(table, i) for i in ids]

    @classmethod
    def paginate(cls,
                 table: str,
                 page: int = 1,
                 per_page: int = 25,
                 order_by: Iterable[str] = (),
                 **conditions) -> List["DynamicModel"]:
        if page < 1:
            page = 1
        offset = (page - 1) * per_page
        cond_sql, cond_vals = cls._build_conditions(conditions)
        q = sql.SQL("SELECT id FROM {}").format(sql.Identifier(table))
        if cond_sql:
            q += sql.SQL(" WHERE {}").format(cond_sql)
        if order_by:
            ob_parts = []
            for c in order_by:
                direction = sql.SQL("DESC") if c.startswith("-") else sql.SQL("ASC")
                ident = sql.Identifier(c.lstrip("-"))
                ob_parts.append(ident + sql.SQL(" ") + direction)
            q += sql.SQL(" ORDER BY ") + sql.SQL(", ").join(ob_parts)
        q += sql.SQL(" LIMIT %s OFFSET %s")
        with cls._get_cursor() as (_, cur):
            cur.execute(q, cond_vals + [per_page, offset])
            ids = [r[0] for r in cur.fetchall()]
        return [cls(table, i) for i in ids]

    # -------------------- DDL-Operationen -------------------------------

    @classmethod
    def add_index(cls, table: str, column: str, unique: bool = False):
        idx_name = f"{table}_{column}_{'uniq' if unique else 'idx'}"
        stmt = sql.SQL("CREATE {uniq} INDEX IF NOT EXISTS {iname} ON {t} ({c})").format(
            uniq=sql.SQL("UNIQUE") if unique else sql.SQL(""),
            iname=sql.Identifier(idx_name),
            t=sql.Identifier(table),
            c=sql.Identifier(column)
        )
        with cls._get_cursor() as (_, cur):
            cur.execute(stmt)

    @classmethod
    def drop_column(cls, table: str, column: str):
        stmt = sql.SQL("ALTER TABLE {t} DROP COLUMN IF EXISTS {c}").format(
            t=sql.Identifier(table),
            c=sql.Identifier(column)
        )
        with cls._get_cursor() as (_, cur):
            cur.execute(stmt)

    @classmethod
    def rename_column(cls, table: str, old: str, new: str):
        stmt = sql.SQL("ALTER TABLE {t} RENAME COLUMN {o} TO {n}").format(
            t=sql.Identifier(table),
            o=sql.Identifier(old),
            n=sql.Identifier(new)
        )
        with cls._get_cursor() as (_, cur):
            cur.execute(stmt)

    # -------------------- Soft Delete -----------------------------------

    def soft_delete(self):
        """
        Setzt ein 'deleted_at' Timestamp-Feld auf NOW() und 'deleted' auf True, statt physisch zu löschen.
        """
        # deleted_at-Feld anlegen falls nicht vorhanden
        if "deleted_at" not in self._columns:
            with self._get_cursor() as (_, cur):
                cur.execute(
                    sql.SQL(
                        "ALTER TABLE {t} ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMP"
                    ).format(t=sql.Identifier(self._table))
                )
            self._columns.add("deleted_at")
        # deleted-Feld anlegen falls nicht vorhanden
        if "deleted" not in self._columns:
            with self._get_cursor() as (_, cur):
                cur.execute(
                    sql.SQL(
                        "ALTER TABLE {t} ADD COLUMN IF NOT EXISTS deleted BOOLEAN DEFAULT FALSE"
                    ).format(t=sql.Identifier(self._table))
                )
            self._columns.add("deleted")

        now = datetime.datetime.utcnow()
        stmt = sql.SQL("UPDATE {t} SET deleted_at = %s, deleted = %s WHERE id = %s").format(
            t=sql.Identifier(self._table)
        )
        with self._get_cursor() as (_, cur):
            cur.execute(stmt, (now, True, self._id))
        self._data["deleted_at"] = now
        self._data["deleted"] = True

    def restore_soft_deleted(self):
        """
        Setzt 'deleted_at' auf NULL und 'deleted' auf False, um einen Datensatz wiederherzustellen.
        """
        if "deleted_at" not in self._columns or "deleted" not in self._columns:
            # Nichts zu tun, wenn Felder fehlen
            return
        stmt = sql.SQL("UPDATE {t} SET deleted_at = NULL, deleted = %s WHERE id = %s").format(
            t=sql.Identifier(self._table)
        )
        with self._get_cursor() as (_, cur):
            cur.execute(stmt, (False, self._id))
        self._data["deleted_at"] = None
        self._data["deleted"] = False

    @classmethod
    def purge_soft_deleted_older_than(cls, table: str, minutes: int) -> int:
        """
        Löscht alle Datensätze mit deleted=True und deleted_at älter als X Minuten endgültig.
        Gibt die Anzahl der gelöschten Zeilen zurück.
        """
        # deleted/deleted_at-Felder anlegen falls nicht vorhanden
        infos = cls.inspect_schema(table)
        columns = {r["column_name"] for r in infos}
        if "deleted" not in columns:
            with cls._get_cursor() as (_, cur):
                cur.execute(
                    sql.SQL(
                        "ALTER TABLE {t} ADD COLUMN IF NOT EXISTS deleted BOOLEAN DEFAULT FALSE"
                    ).format(t=sql.Identifier(table))
                )
        if "deleted_at" not in columns:
            with cls._get_cursor() as (_, cur):
                cur.execute(
                    sql.SQL(
                        "ALTER TABLE {t} ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMP"
                    ).format(t=sql.Identifier(table))
                )
        stmt = sql.SQL(
            "DELETE FROM {t} WHERE deleted = TRUE AND deleted_at IS NOT NULL AND deleted_at < (NOW() - INTERVAL %s MINUTE)"
        ).format(t=sql.Identifier(table))
        with cls._get_cursor() as (_, cur):
            cur.execute(stmt, (minutes,))
            return cur.rowcount

    # TODO: 
    # - Optional: Soft-Delete für mehrere Tabellen gleichzeitig ermöglichen
    # - Optional: Soft-Delete-Status in Queries automatisch berücksichtigen (z.B. nur nicht-gelöschte laden)
    # - Optional: Automatische Zeitsteuerung für purge (z.B. per Scheduler)
    # ...existing code...

    # -------------------- Transaction-Context --------------------------

    @classmethod
    @contextlib.contextmanager
    def transaction(cls):
        """
        with DynamicModel.transaction():
            … mehrere Operationen …
        commit/rollback automatisch
        """
        if cls._connection is None and cls._pool is None:
            raise RuntimeError("Keine Datenbankverbindung.")
        conn = cls._pool.getconn() if cls._pool else cls._connection
        try:
            yield
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            if cls._pool:
                cls._pool.putconn(conn)

    # -------------------- Raw SQL ---------------------------------------

    @classmethod
    def raw_query(cls, query: str, params: Iterable[Any] = ()) -> List[Dict[str, Any]]:
        """
        Führt beliebiges SQL aus, gibt List[Dict] zurück (oder [] ohne Ergebnis).
        """
        with cls._get_cursor(dict_cursor=True) as (_, cur):
            cur.execute(query, params)
            try:
                return cur.fetchall()
            except psycopg2.ProgrammingError:
                return []

    # -------------------- Simple Relationship --------------------------

    def children(self, child_table: str, fk_column: str) -> List["DynamicModel"]:
        """
        parent.children("orders", fk_column="user_id")
        """
        ids = DynamicModel.find_ids(child_table, **{fk_column: self._id})
        return [DynamicModel(child_table, i) for i in ids]

    # -------------------- Migration-Framework -------------------------

    @classmethod
    def add_migration(cls, name: str, fn: Callable[[], None]):
        """
        Registriert eine Migrations-Funktion, die einmalig ausgeführt wird.
        """
        cls._migrations.append((name, fn))

    @classmethod
    def run_migrations(cls):
        """
        Führt alle registrierten Migrationen in Transaktionen aus.
        """
        for name, fn in cls._migrations:
            print(f"Running migration: {name}")
            with cls.transaction():
                fn()

    # -------------------- Instanz-Logik (Rest des ORMs) ---------------

    def __init__(self, table: str, row_id: int):
        if self._connection is None and self._pool is None:
            raise RuntimeError("Bitte erst DynamicModel.connect() aufrufen.")
        self._table = table
        self._id = row_id
        self._columns: set = set()
        self._data: dict = {}
        self._load_columns()
        self._load_data()

    def _load_columns(self):
        infos = self.inspect_schema(self._table)
        if not infos:
            raise ValueError(f"Tabelle '{self._table}' existiert nicht.")
        self._columns = {r["column_name"] for r in infos}

    def _load_data(self):
        cols_sql = sql.SQL(", ").join(map(sql.Identifier, self._columns))
        q = sql.SQL("SELECT {cols} FROM {t} WHERE id = %s").format(
            cols=cols_sql,
            t=sql.Identifier(self._table)
        )
        with self._get_cursor(dict_cursor=True) as (_, cur):
            cur.execute(q, (self._id,))
            row = cur.fetchone()
        if not row:
            raise ValueError(f"Kein Datensatz mit id={self._id} in Tabelle {self._table}.")
        self._data = dict(row)

    def __getattr__(self, name: str) -> Any:
        if name in self._columns:
            return self._data.get(name)
        raise AttributeError(f"'{type(self).__name__}' hat kein Attribut '{name}'")

    def __setattr__(self, name: str, value: Any):
        # interne Felder weiterreichen
        if name in ("_table", "_id", "_columns", "_data",
                    "_connection", "_pool",
                    "_before_hooks", "_after_hooks", "_migrations"):
            return super().__setattr__(name, value)

        # Update einer existierenden Spalte
        if name in self._columns:
            stmt = sql.SQL("UPDATE {t} SET {c} = %s WHERE id = %s").format(
                t=sql.Identifier(self._table),
                c=sql.Identifier(name)
            )
            with self._get_cursor() as (_, cur):
                cur.execute(stmt, (value, self._id))
            self._data[name] = value
            return

        # Neue Spalte hinzufügen
        with self._get_cursor() as (_, cur):
            cur.execute(
                sql.SQL("ALTER TABLE {t} ADD COLUMN {c} TEXT").format(
                    t=sql.Identifier(self._table),
                    c=sql.Identifier(name)
                )
            )
        self._columns.add(name)
        # Dann befüllen
        stmt = sql.SQL("UPDATE {t} SET {c} = %s WHERE id = %s").format(
            t=sql.Identifier(self._table),
            c=sql.Identifier(name)
        )
        with self._get_cursor() as (_, cur):
            cur.execute(stmt, (value, self._id))
        self._data[name] = value

    def save(self):
        """
        Speichert alle im _data gehaltenen Werte (außer id).
        """
        cols = [c for c in self._columns if c != "id"]
        if not cols:
            return
        parts = [
            sql.SQL("{} = %s").format(sql.Identifier(c))
            for c in cols
        ]
        stmt = sql.SQL("UPDATE {t} SET {sets} WHERE id = %s").format(
            t=sql.Identifier(self._table),
            sets=sql.SQL(", ").join(parts)
        )
        vals = [self._data[c] for c in cols] + [self._id]
        with self._get_cursor() as (_, cur):
            cur.execute(stmt, vals)

    def delete(self):
        """
        Physisches Löschen dieses Datensatzes.
        """
        stmt = sql.SQL("DELETE FROM {t} WHERE id = %s").format(
            t=sql.Identifier(self._table)
        )
        with self._get_cursor() as (_, cur):
            cur.execute(stmt, (self._id,))
    @classmethod
    def list_all_ids(cls, table: str) -> List[int]:
        q = sql.SQL("SELECT id FROM {}").format(sql.Identifier(table))
        with cls._get_cursor() as (_, cur):
            cur.execute(q)
            return [r[0] for r in cur.fetchall()]

    # -------------------- Update/Delete by Conditions ---------------------

    @classmethod
    def update_by_conditions(cls,
                             table: str,
                             updates: Dict[str, Any],
                             **conditions) -> int:
        """
        UPDATE … SET … WHERE …; gibt rowcount zurück.
        """
        if not updates:
            return 0
        set_parts, set_vals = [], []
        for col, val in updates.items():
            set_parts.append(sql.SQL("{} = %s").format(sql.Identifier(col)))
            set_vals.append(val)
        cond_sql, cond_vals = cls._build_conditions(conditions)
        q = sql.SQL("UPDATE {} SET {}").format(
            sql.Identifier(table),
            sql.SQL(", ").join(set_parts)
        )
        if cond_sql:
            q += sql.SQL(" WHERE {}").format(cond_sql)
        with cls._get_cursor() as (_, cur):
            cur.execute(q, set_vals + cond_vals)
            return cur.rowcount

    @classmethod
    def delete_by_conditions(cls, table: str, **conditions) -> int:
        """
        DELETE FROM … WHERE …; gibt rowcount zurück.
        """
        cond_sql, cond_vals = cls._build_conditions(conditions)
        q = sql.SQL("DELETE FROM {}").format(sql.Identifier(table))
        if cond_sql:
            q += sql.SQL(" WHERE {}").format(cond_sql)
        with cls._get_cursor() as (_, cur):
            cur.execute(q, cond_vals)
            return cur.rowcount

    # -------------------- Count / Exists -------------------------------

    @classmethod
    def count(cls, table: str, **conditions) -> int:
        cond_sql, cond_vals = cls._build_conditions(conditions)
        q = sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(table))
        if cond_sql:
            q += sql.SQL(" WHERE {}").format(cond_sql)
        with cls._get_cursor() as (_, cur):
            cur.execute(q, cond_vals)
            return cur.fetchone()[0]

    @classmethod
    def exists(cls, table: str, **conditions) -> bool:
        return cls.count(table, **conditions) > 0

    # -------------------- get_all / paginate ----------------------------

    @classmethod
    def get_all(cls,
                table: str,
                order_by: Iterable[str] = (),
                **conditions) -> List["DynamicModel"]:
        ids = cls.find_ids(table, order_by=order_by, **conditions)
        return [cls(table, i) for i in ids]

    @classmethod
    def paginate(cls,
                 table: str,
                 page: int = 1,
                 per_page: int = 25,
                 order_by: Iterable[str] = (),
                 **conditions) -> List["DynamicModel"]:
        if page < 1:
            page = 1
        offset = (page - 1) * per_page
        cond_sql, cond_vals = cls._build_conditions(conditions)
        q = sql.SQL("SELECT id FROM {}").format(sql.Identifier(table))
        if cond_sql:
            q += sql.SQL(" WHERE {}").format(cond_sql)
        if order_by:
            ob_parts = []
            for c in order_by:
                direction = sql.SQL("DESC") if c.startswith("-") else sql.SQL("ASC")
                ident = sql.Identifier(c.lstrip("-"))
                ob_parts.append(ident + sql.SQL(" ") + direction)
            q += sql.SQL(" ORDER BY ") + sql.SQL(", ").join(ob_parts)
        q += sql.SQL(" LIMIT %s OFFSET %s")
        with cls._get_cursor() as (_, cur):
            cur.execute(q, cond_vals + [per_page, offset])
            ids = [r[0] for r in cur.fetchall()]
        return [cls(table, i) for i in ids]

    # -------------------- DDL-Operationen -------------------------------

    @classmethod
    def add_index(cls, table: str, column: str, unique: bool = False):
        idx_name = f"{table}_{column}_{'uniq' if unique else 'idx'}"
        stmt = sql.SQL("CREATE {uniq} INDEX IF NOT EXISTS {iname} ON {t} ({c})").format(
            uniq=sql.SQL("UNIQUE") if unique else sql.SQL(""),
            iname=sql.Identifier(idx_name),
            t=sql.Identifier(table),
            c=sql.Identifier(column)
        )
        with cls._get_cursor() as (_, cur):
            cur.execute(stmt)

    @classmethod
    def drop_column(cls, table: str, column: str):
        stmt = sql.SQL("ALTER TABLE {t} DROP COLUMN IF EXISTS {c}").format(
            t=sql.Identifier(table),
            c=sql.Identifier(column)
        )
        with cls._get_cursor() as (_, cur):
            cur.execute(stmt)

    @classmethod
    def rename_column(cls, table: str, old: str, new: str):
        stmt = sql.SQL("ALTER TABLE {t} RENAME COLUMN {o} TO {n}").format(
            t=sql.Identifier(table),
            o=sql.Identifier(old),
            n=sql.Identifier(new)
        )
        with cls._get_cursor() as (_, cur):
            cur.execute(stmt)

    # -------------------- Soft Delete -----------------------------------

    def soft_delete(self):
        """
        Setzt ein 'deleted_at' Timestamp-Feld auf NOW() und 'deleted' auf True, statt physisch zu löschen.
        """
        # deleted_at-Feld anlegen falls nicht vorhanden
        if "deleted_at" not in self._columns:
            with self._get_cursor() as (_, cur):
                cur.execute(
                    sql.SQL(
                        "ALTER TABLE {t} ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMP"
                    ).format(t=sql.Identifier(self._table))
                )
            self._columns.add("deleted_at")
        # deleted-Feld anlegen falls nicht vorhanden
        if "deleted" not in self._columns:
            with self._get_cursor() as (_, cur):
                cur.execute(
                    sql.SQL(
                        "ALTER TABLE {t} ADD COLUMN IF NOT EXISTS deleted BOOLEAN DEFAULT FALSE"
                    ).format(t=sql.Identifier(self._table))
                )
            self._columns.add("deleted")

        now = datetime.datetime.utcnow()
        stmt = sql.SQL("UPDATE {t} SET deleted_at = %s, deleted = %s WHERE id = %s").format(
            t=sql.Identifier(self._table)
        )
        with self._get_cursor() as (_, cur):
            cur.execute(stmt, (now, True, self._id))
        self._data["deleted_at"] = now
        self._data["deleted"] = True

    def restore_soft_deleted(self):
        """
        Setzt 'deleted_at' auf NULL und 'deleted' auf False, um einen Datensatz wiederherzustellen.
        """
        if "deleted_at" not in self._columns or "deleted" not in self._columns:
            # Nichts zu tun, wenn Felder fehlen
            return
        stmt = sql.SQL("UPDATE {t} SET deleted_at = NULL, deleted = %s WHERE id = %s").format(
            t=sql.Identifier(self._table)
        )
        with self._get_cursor() as (_, cur):
            cur.execute(stmt, (False, self._id))
        self._data["deleted_at"] = None
        self._data["deleted"] = False

    @classmethod
    def purge_soft_deleted_older_than(cls, table: str, minutes: int) -> int:
        """
        Löscht alle Datensätze mit deleted=True und deleted_at älter als X Minuten endgültig.
        Gibt die Anzahl der gelöschten Zeilen zurück.
        """
        # deleted/deleted_at-Felder anlegen falls nicht vorhanden
        infos = cls.inspect_schema(table)
        columns = {r["column_name"] for r in infos}
        if "deleted" not in columns:
            with cls._get_cursor() as (_, cur):
                cur.execute(
                    sql.SQL(
                        "ALTER TABLE {t} ADD COLUMN IF NOT EXISTS deleted BOOLEAN DEFAULT FALSE"
                    ).format(t=sql.Identifier(table))
                )
        if "deleted_at" not in columns:
            with cls._get_cursor() as (_, cur):
                cur.execute(
                    sql.SQL(
                        "ALTER TABLE {t} ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMP"
                    ).format(t=sql.Identifier(table))
                )
        stmt = sql.SQL(
            "DELETE FROM {t} WHERE deleted = TRUE AND deleted_at IS NOT NULL AND deleted_at < (NOW() - INTERVAL %s MINUTE)"
        ).format(t=sql.Identifier(table))
        with cls._get_cursor() as (_, cur):
            cur.execute(stmt, (minutes,))
            return cur.rowcount

    # TODO: 
    # - Optional: Soft-Delete für mehrere Tabellen gleichzeitig ermöglichen
    # - Optional: Soft-Delete-Status in Queries automatisch berücksichtigen (z.B. nur nicht-gelöschte laden)
    # - Optional: Automatische Zeitsteuerung für purge (z.B. per Scheduler)
    # ...existing code...

    # -------------------- Transaction-Context --------------------------

    @classmethod
    @contextlib.contextmanager
    def transaction(cls):
        """
        with DynamicModel.transaction():
            … mehrere Operationen …
        commit/rollback automatisch
        """
        if cls._connection is None and cls._pool is None:
            raise RuntimeError("Keine Datenbankverbindung.")
        conn = cls._pool.getconn() if cls._pool else cls._connection
        try:
            yield
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            if cls._pool:
                cls._pool.putconn(conn)

    # -------------------- Raw SQL ---------------------------------------

    @classmethod
    def raw_query(cls, query: str, params: Iterable[Any] = ()) -> List[Dict[str, Any]]:
        """
        Führt beliebiges SQL aus, gibt List[Dict] zurück (oder [] ohne Ergebnis).
        """
        with cls._get_cursor(dict_cursor=True) as (_, cur):
            cur.execute(query, params)
            try:
                return cur.fetchall()
            except psycopg2.ProgrammingError:
                return []

    # -------------------- Simple Relationship --------------------------

    def children(self, child_table: str, fk_column: str) -> List["DynamicModel"]:
        """
        parent.children("orders", fk_column="user_id")
        """
        ids = DynamicModel.find_ids(child_table, **{fk_column: self._id})
        return [DynamicModel(child_table, i) for i in ids]

    # -------------------- Migration-Framework -------------------------

    @classmethod
    def add_migration(cls, name: str, fn: Callable[[], None]):
        """
        Registriert eine Migrations-Funktion, die einmalig ausgeführt wird.
        """
        cls._migrations.append((name, fn))

    @classmethod
    def run_migrations(cls):
        """
        Führt alle registrierten Migrationen in Transaktionen aus.
        """
        for name, fn in cls._migrations:
            print(f"Running migration: {name}")
            with cls.transaction():
                fn()

    # -------------------- Instanz-Logik (Rest des ORMs) ---------------

    def __init__(self, table: str, row_id: int):
        if self._connection is None and self._pool is None:
            raise RuntimeError("Bitte erst DynamicModel.connect() aufrufen.")
        self._table = table
        self._id = row_id
        self._columns: set = set()
        self._data: dict = {}
        self._load_columns()
        self._load_data()

    def _load_columns(self):
        infos = self.inspect_schema(self._table)
        if not infos:
            raise ValueError(f"Tabelle '{self._table}' existiert nicht.")
        self._columns = {r["column_name"] for r in infos}

    def _load_data(self):
        cols_sql = sql.SQL(", ").join(map(sql.Identifier, self._columns))
        q = sql.SQL("SELECT {cols} FROM {t} WHERE id = %s").format(
            cols=cols_sql,
            t=sql.Identifier(self._table)
        )
        with self._get_cursor(dict_cursor=True) as (_, cur):
            cur.execute(q, (self._id,))
            row = cur.fetchone()
        if not row:
            raise ValueError(f"Kein Datensatz mit id={self._id} in Tabelle {self._table}.")
        self._data = dict(row)

    def __getattr__(self, name: str) -> Any:
        if name in self._columns:
            return self._data.get(name)
        raise AttributeError(f"'{type(self).__name__}' hat kein Attribut '{name}'")

    def __setattr__(self, name: str, value: Any):
        # interne Felder weiterreichen
        if name in ("_table", "_id", "_columns", "_data",
                    "_connection", "_pool",
                    "_before_hooks", "_after_hooks", "_migrations"):
            return super().__setattr__(name, value)

        # Update einer existierenden Spalte
        if name in self._columns:
            stmt = sql.SQL("UPDATE {t} SET {c} = %s WHERE id = %s").format(
                t=sql.Identifier(self._table),
                c=sql.Identifier(name)
            )
            with self._get_cursor() as (_, cur):
                cur.execute(stmt, (value, self._id))
            self._data[name] = value
            return

        # Neue Spalte hinzufügen
        with self._get_cursor() as (_, cur):
            cur.execute(
                sql.SQL("ALTER TABLE {t} ADD COLUMN {c} TEXT").format(
                    t=sql.Identifier(self._table),
                    c=sql.Identifier(name)
                )
            )
        self._columns.add(name)
        # Dann befüllen
        stmt = sql.SQL("UPDATE {t} SET {c} = %s WHERE id = %s").format(
            t=sql.Identifier(self._table),
            c=sql.Identifier(name)
        )
        with self._get_cursor() as (_, cur):
            cur.execute(stmt, (value, self._id))
        self._data[name] = value

    def save(self):
        """
        Speichert alle im _data gehaltenen Werte (außer id).
        """
        cols = [c for c in self._columns if c != "id"]
        if not cols:
            return
        parts = [
            sql.SQL("{} = %s").format(sql.Identifier(c))
            for c in cols
        ]
        stmt = sql.SQL("UPDATE {t} SET {sets} WHERE id = %s").format(
            t=sql.Identifier(self._table),
            sets=sql.SQL(", ").join(parts)
        )
        vals = [self._data[c] for c in cols] + [self._id]
        with self._get_cursor() as (_, cur):
            cur.execute(stmt, vals)

    def delete(self):
        """
        Physisches Löschen dieses Datensatzes.
        """
        stmt = sql.SQL("DELETE FROM {t} WHERE id = %s").format(
            t=sql.Identifier(self._table)
        )
        with self._get_cursor() as (_, cur):
            cur.execute(stmt, (self._id,))
    # Mutationen sind erlaubt (z. B. Standardwerte setzen)
    data.setdefault("created_from", "script")

def after_user_insert(data: dict):
    print("neuer user:", data.get("email"))

DM.register_before_insert("*", before_any_insert)
DM.register_after_insert("users", after_user_insert)
```

Semantik:
- BEFORE‑Hooks: Global (`"*"`/None) und tabellenspezifisch (Reihenfolge: global → spezifisch). Exceptions beenden den Insert.
- AFTER‑Hooks: Exceptions werden verschluckt (dürfen Workflow nicht stören).



## Schema‑Caching & Typ‑Inference

- Schema‑Cache: `inspect_schema()` cached Ergebnisse für `set_schema_cache_ttl(seconds)` (Default: 300).
  - 0 Sekunden bedeutet: Cache läuft nie ab.
- Typ‑Inference bei neuen Spalten:
  - bool → BOOLEAN
  - int → BIGINT
  - float → DOUBLE PRECISION
  - decimal.Decimal → NUMERIC
  - datetime → TIMESTAMP
  - date → DATE
  - dict/list → JSONB
  - bytes/bytearray/memoryview → BYTEA
  - sonst → TEXT
- Du kannst Spaltentypen explizit angeben: `create(..., column_types={"field": "UUID"})`.



## Performance‑Tipps

- Verwende `bulk_create` und `bulk_update` für große Mengen.
- Indexe/Constraints über DDL‑Helper setzen (z. B. `add_index`, `add_unique`).
- `stream_query` für riesige Resultsets.
- Schema‑Cache (Default 5 Min.) reduziert Overhead bei häufigen Schemaabfragen.
- `execute_batch` für wiederholte parametrische Befehle.



## Sicherheit & Fehlerverhalten

- SQL‑Sicherheit:
  - Spalten- und Tabellennamen werden via `sql.Identifier` geschützt.
  - Werte werden als Parameter gebunden (`%s` Platzhalter).
  - Vorsicht mit `aggregate(func=...)`: `func` wird als SQL aufgenommen. Keine unvalidierten User‑Inputs dort verwenden.
  - `raw_query`: immer Parameter binden, nicht string-interpolieren.

- Transaktionen:
  - `transaction()` und `savepoint()` kümmern sich um commit/rollback. Exceptions lösen Rollbacks aus.

- Hooks:
  - BEFORE‑Hook‑Exceptions schlagen durch (bewusst). AFTER‑Hook‑Exceptions werden ignoriert.

- Typen:
  - Automatisches Anlegen neuer Spalten ist mächtig, aber sollte bewusst eingesetzt werden (Schema‑Kontrolle/Reviews).
  - Standardmäßig wird für unbekannte Typen TEXT gewählt.



## FAQ

- Wie sortiere ich absteigend?
  - `order_by=("-id",)`

- Wie filtere ich mehrere Werte?
  - `find_ids("users", status={"active", "blocked"})` nutzt `status = ANY(%s)`.

- Wie funktioniert Upsert?
  ```python
  DM.add_unique("users", ["email"])  # oder bestehender Unique Index
  user_id = DM.upsert(
      "users",
      conflict_cols=["email"],
      values={"email": "a@b.c", "name": "Ada"},
  )
  ```

- Wie nutze ich Optimistic Locking?
  ```python
  DM.ensure_version_column("users")
  u = DM.first("users")
  ok = u.save_with_version()  # False bei Versionskonflikt
  ```

- Warum sehe ich gelöschte Datensätze nicht?
  - Standardfilter `exclude_deleted=True`. Deaktiviere mit `exclude_deleted=False`:
    `DM.get_all("users", exclude_deleted=False)`.

- Wie implementiere ich Migrationen?
  ```python
  def m_add_phone():
      DM.ensure_columns("users", {"phone": "TEXT"})
  DM.add_migration("002_add_phone", m_add_phone)
  DM.run_migrations()
  ```



## Cheat‑Sheet

```python
DM.connect_pool(minconn=1, maxconn=5, host="...", dbname="...", user="...", password="...")

# Inserts
u = DM.create("users", email="a@b.c", name="Ada")
ids = DM.bulk_create("users", [{"email": "x@y.z"}, {"email": "y@z.w"}])
uid = DM.upsert("users", conflict_cols=["email"], values={"email":"a@b.c", "name":"Dr. Ada"})

# Reads
u = DM.get_by("users", email="a@b.c")
users = DM.get_all("users", order_by=("-id",))
page, total = DM.paginate_with_count("users", page=2, per_page=20)

# Updates
DM.update_by_conditions("users", {"name": "New"}, email="a@b.c")
DM.bulk_update("users", [{"id":1, "name":"A"}, {"id":2, "name":"B"}])

# Deletes
DM.delete_by_conditions("users", id=5)

# Soft-Delete
u.soft_delete()
DM.purge_soft_deleted_older_than("users", minutes=60)

# Transactions
with DM.transaction():
    # ...
    pass

# DDL
DM.add_index("users", "email", unique=True)
DM.add_foreign_key("orders", "user_id", "users")

# Audit
DM.enable_audit_trail("users")

# Raw/Streaming
rows = DM.raw_query("SELECT * FROM users WHERE email=%s", ["a@b.c"])
for r in DM.stream_query("SELECT * FROM big_table"):
    ...

DM.close()
```

Viel Erfolg mit DynamicModel! Wenn du spezielle Erweiterungen brauchst (Operatoren in WHERE, zusammengesetzte OrderBys, eigene Typ-Mappings, etc.), kann das gezielt ergänzt werden.
