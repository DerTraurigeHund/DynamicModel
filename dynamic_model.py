from __future__ import annotations

import contextlib
import datetime
import json
import threading
import time
import decimal
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
)

import psycopg2
from psycopg2 import sql
import psycopg2.extras
from psycopg2.pool import SimpleConnectionPool


class DynamicModel:
    """
    Mini-ORM mit umfangreichen CRUD-, DDL- und Utility-Methoden,
    Connection-Pooling, Hooks, Migrationen (mit Historie), Soft-Delete, u.v.m.
    """

    # --- Klassenattribute / State ---
    _connection: Optional[psycopg2.extensions.connection] = None
    _pool: Optional[SimpleConnectionPool] = None

    # Hooks: tabellenspezifisch und global ("*")
    _before_hooks: Dict[str, List[Callable[[dict], None]]] = {}
    _after_hooks: Dict[str, List[Callable[[dict], None]]] = {}

    # Migrationen (Name, Funktion)
    _migrations: List[Tuple[str, Callable[[], None]]] = []

    # Logger
    _logger: Optional[Callable[[str, Sequence[Any]], None]] = None

    # Thread-Local für Transaktionen
    _local = threading.local()

    # Schema-Cache (TTL)
    _schema_cache: Dict[str, Tuple[float, List[Dict[str, Any]]]] = {}
    _schema_cache_ttl_seconds: int = 300

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
    def close(cls) -> None:
        """
        Schließt Einzelverbindung oder Pool (falls vorhanden).
        """
        if cls._pool is not None:
            try:
                cls._pool.closeall()
            finally:
                cls._pool = None
        if cls._connection is not None:
            try:
                cls._connection.close()
            finally:
                cls._connection = None

    # Alias
    close_pool = close

    # ---------------------- Logger ---------------------------------------

    @classmethod
    def set_logger(cls, fn: Optional[Callable[[str, Sequence[Any]], None]]):
        """
        Setzt/entfernt einen Query-Logger: fn(sql_text, params).
        """
        cls._logger = fn

    @classmethod
    def _log_sql(cls, conn, query: Any, params: Optional[Sequence[Any]]) -> None:
        if not cls._logger:
            return
        try:
            if isinstance(query, (sql.SQL, sql.Composed)):
                q = query.as_string(conn)
            else:
                q = str(query)
        except Exception:
            q = str(query)
        try:
            cls._logger(q, params or [])
        except Exception:
            # Logger darf niemals stören
            pass

    # ---------------------- Helpers für Connection / Cursor ---------------

    @classmethod
    def _in_transaction(cls) -> bool:
        return getattr(cls._local, "conn", None) is not None

    @classmethod
    def _current_connection(cls) -> psycopg2.extensions.connection:
        if cls._in_transaction():
            return cls._local.conn  # type: ignore[attr-defined]
        if cls._connection is None and cls._pool is None:
            raise RuntimeError("Bitte erst DynamicModel.connect() oder connect_pool() aufrufen.")
        if cls._pool:
            return cls._pool.getconn()
        assert cls._connection is not None
        return cls._connection

    @classmethod
    def _release_connection(cls, conn: psycopg2.extensions.connection):
        if cls._pool and not cls._in_transaction():
            cls._pool.putconn(conn)

    @classmethod
    @contextlib.contextmanager
    def _get_cursor(cls, dict_cursor: bool = False):
        """
        Interner Context-Manager, der je nach Setup aus Pool/EZ-Verbindung greift.
        Er commit/rollback nur, wenn NICHT in einer (äußeren) transaction().
        """
        if cls._connection is None and cls._pool is None:
            raise RuntimeError("Bitte erst DynamicModel.connect() oder connect_pool() aufrufen.")

        conn = cls._current_connection()
        cur_cls = psycopg2.extras.RealDictCursor if dict_cursor else None
        cur = conn.cursor(cursor_factory=cur_cls)
        try:
            yield conn, cur
            if not cls._in_transaction():
                conn.commit()
        except Exception:
            try:
                if not cls._in_transaction():
                    conn.rollback()
            finally:
                raise
        finally:
            try:
                cur.close()
            finally:
                if not cls._in_transaction():
                    cls._release_connection(conn)

    # ----------------------- Hook-System ---------------------------------

    @classmethod
    def register_before_insert(cls, table: Optional[str], fn: Callable[[dict], None]):
        """
        table = "tabelle" oder "*" (global) oder None (global)
        """
        key = table or "*"
        cls._before_hooks.setdefault(key, []).append(fn)

    @classmethod
    def register_after_insert(cls, table: Optional[str], fn: Callable[[dict], None]):
        key = table or "*"
        cls._after_hooks.setdefault(key, []).append(fn)

    @classmethod
    def _run_before_hooks(cls, table: str, row: dict):
        # Erst globale, dann tabellenspezifische
        for key in ("*", table):
            for fn in cls._before_hooks.get(key, []):
                try:
                    fn(row)
                except Exception as e:
                    raise RuntimeError(f"Fehler im BEFORE-Hook ({key}): {e}") from e

    @classmethod
    def _run_after_hooks(cls, table: str, row: dict):
        for key in ("*", table):
            for fn in cls._after_hooks.get(key, []):
                try:
                    fn(row)
                except Exception:
                    # After-Hooks sollen die Logik nicht zerlegen
                    pass

    # -------------------- Schema-Cache / Inspektion -----------------------

    @classmethod
    def set_schema_cache_ttl(cls, seconds: int) -> None:
        cls._schema_cache_ttl_seconds = max(0, seconds)

    @classmethod
    def _cache_key(cls, table: str) -> str:
        return f"public.{table}"

    @classmethod
    def _invalidate_schema_cache(cls, table: str) -> None:
        cls._schema_cache.pop(cls._cache_key(table), None)

    @classmethod
    def inspect_schema(cls, table: str) -> List[Dict[str, Any]]:
        """
        Gibt Metadaten zu Spalten zurück: name, type, nullable, default…
        mit TTL-Cache.
        """
        key = cls._cache_key(table)
        now = time.time()
        if key in cls._schema_cache:
            ts, infos = cls._schema_cache[key]
            if cls._schema_cache_ttl_seconds == 0 or (now - ts) < cls._schema_cache_ttl_seconds:
                return infos

        qry = """
            SELECT column_name, data_type, is_nullable, column_default
              FROM information_schema.columns
             WHERE table_schema = 'public' AND table_name = %s
             ORDER BY ordinal_position
        """
        with cls._get_cursor(dict_cursor=True) as (_, cur):
            cls._log_sql(_, qry, (table,))
            cur.execute(qry, (table,))
            rows = cur.fetchall()
            cls._schema_cache[key] = (now, rows)
            return rows

    @classmethod
    def _has_column(cls, table: str, column: str) -> bool:
        infos = cls.inspect_schema(table)
        return any(r["column_name"] == column for r in infos)

    @classmethod
    def ensure_columns(cls, table: str, columns: Dict[str, str]) -> None:
        """
        Fügt mehrere Spalten mit expliziten SQL-Typen hinzu (falls nicht vorhanden).
        columns: {column: "SQLTYPE [DEFAULT ...] [NOT NULL]"}
        """
        if not columns:
            return
        stmts = []
        for col, typ in columns.items():
            stmts.append(
                sql.SQL("ADD COLUMN IF NOT EXISTS {} {}").format(
                    sql.Identifier(col), sql.SQL(typ)
                )
            )
        stmt = sql.SQL("ALTER TABLE {} {}").format(
            sql.Identifier(table),
            sql.SQL(", ").join(stmts)
        )
        with cls._get_cursor() as (conn, cur):
            cls._log_sql(conn, stmt, None)
            cur.execute(stmt)
        cls._invalidate_schema_cache(table)

    # -------------------- Type-Inference ----------------------------------

    @classmethod
    def _infer_pg_type(cls, value: Any) -> str:
        if value is None:
            return "TEXT"
        if isinstance(value, bool):
            return "BOOLEAN"
        if isinstance(value, int):
            return "BIGINT"
        if isinstance(value, float):
            return "DOUBLE PRECISION"
        if isinstance(value, decimal.Decimal):
            return "NUMERIC"
        if isinstance(value, (datetime.datetime,)):
            return "TIMESTAMP"
        if isinstance(value, (datetime.date,)):
            return "DATE"
        if isinstance(value, (dict, list)):
            return "JSONB"
        if isinstance(value, (bytes, bytearray, memoryview)):
            return "BYTEA"
        return "TEXT"

    # -------------------- Tabellen-Definition -----------------------------

    @classmethod
    def create_table(cls, table: str, schema: Dict[str, str]):
        """
        CREATE TABLE IF NOT EXISTS … plus automatisch 'id SERIAL PRIMARY KEY'.
        schema: dict of column -> SQL-Type.
        """
        cols = [sql.SQL("id SERIAL PRIMARY KEY")]
        for col, typ in schema.items():
            cols.append(sql.SQL("{} {}").format(sql.Identifier(col), sql.SQL(typ)))
        q = sql.SQL("CREATE TABLE IF NOT EXISTS {} ({})").format(
            sql.Identifier(table),
            sql.SQL(", ").join(cols),
        )
        with cls._get_cursor() as (conn, cur):
            cls._log_sql(conn, q, None)
            cur.execute(q)
        cls._invalidate_schema_cache(table)

    @classmethod
    def drop_table(cls, table: str, cascade: bool = False):
        stmt = sql.SQL("DROP TABLE IF EXISTS {} {}").format(
            sql.Identifier(table),
            sql.SQL("CASCADE" if cascade else "")
        )
        with cls._get_cursor() as (conn, cur):
            cls._log_sql(conn, stmt, None)
            cur.execute(stmt)
        cls._invalidate_schema_cache(table)

    # -------------------- Hilfsfunktionen für WHERE -----------------------

    @classmethod
    def _build_conditions(cls, conditions: Dict[str, Any]) -> Tuple[sql.SQL, List[Any]]:
        if not conditions:
            return sql.SQL(""), []
        parts, vals = [], []
        for col, val in conditions.items():
            if isinstance(val, (list, tuple, set)):
                parts.append(sql.SQL("{} = ANY(%s)").format(sql.Identifier(col)))
                vals.append(list(val))
            else:
                parts.append(sql.SQL("{} = %s").format(sql.Identifier(col)))
                vals.append(val)
        return sql.SQL(" AND ").join(parts), vals

    @classmethod
    def _append_soft_delete_filter(
        cls,
        table: str,
        base_sql: sql.SQL,
        cond_sql: sql.SQL,
        cond_vals: List[Any],
        exclude_deleted: bool,
    ) -> Tuple[sql.SQL, List[Any]]:
        """
        Hängt an WHERE-Klauseln zusätzliche Filter für Soft-Delete an, wenn Spalte 'deleted' existiert.
        """
        vals = list(cond_vals)
        if exclude_deleted and cls._has_column(table, "deleted"):
            soft = sql.SQL("COALESCE(deleted, FALSE) = FALSE")
            if cond_sql:
                cond_sql = sql.SQL("{} AND {}").format(cond_sql, soft)
            else:
                cond_sql = soft
        if cond_sql:
            base_sql += sql.SQL(" WHERE {}").format(cond_sql)
        return base_sql, vals

    # -------------------- SELECT Hilfen -----------------------------------

    @classmethod
    def find_ids(
        cls,
        table: str,
        order_by: Iterable[str] = (),
        exclude_deleted: bool = True,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        **conditions,
    ) -> List[int]:
        """
        SELECT id FROM … WHERE … ORDER BY … LIMIT/OFFSET …
        """
        cond_sql, cond_vals = cls._build_conditions(conditions)
        q = sql.SQL("SELECT id FROM {}").format(sql.Identifier(table))
        q, cond_vals = cls._append_soft_delete_filter(table, q, cond_sql, cond_vals, exclude_deleted)

        if order_by:
            ob_parts = []
            for c in order_by:
                direction = sql.SQL("DESC") if str(c).startswith("-") else sql.SQL("ASC")
                ident = sql.Identifier(str(c).lstrip("-"))
                ob_parts.append(ident + sql.SQL(" ") + direction)
            q += sql.SQL(" ORDER BY ") + sql.SQL(", ").join(ob_parts)

        if limit is not None:
            q += sql.SQL(" LIMIT %s")
            cond_vals.append(limit)
        if offset is not None:
            q += sql.SQL(" OFFSET %s")
            cond_vals.append(offset)

        with cls._get_cursor() as (conn, cur):
            cls._log_sql(conn, q, cond_vals)
            cur.execute(q, cond_vals)
            return [r[0] for r in cur.fetchall()]

    @classmethod
    def list_all_ids(cls, table: str, exclude_deleted: bool = True) -> List[int]:
        return cls.find_ids(table, exclude_deleted=exclude_deleted)

    @classmethod
    def get_all(
        cls,
        table: str,
        order_by: Iterable[str] = (),
        exclude_deleted: bool = True,
        **conditions,
    ) -> List["DynamicModel"]:
        ids = cls.find_ids(table, order_by=order_by, exclude_deleted=exclude_deleted, **conditions)
        return [cls(table, i) for i in ids]

    @classmethod
    def paginate(
        cls,
        table: str,
        page: int = 1,
        per_page: int = 25,
        order_by: Iterable[str] = (),
        exclude_deleted: bool = True,
        **conditions,
    ) -> List["DynamicModel"]:
        if page < 1:
            page = 1
        offset = (page - 1) * per_page
        ids = cls.find_ids(
            table,
            order_by=order_by,
            exclude_deleted=exclude_deleted,
            limit=per_page,
            offset=offset,
            **conditions,
        )
        return [cls(table, i) for i in ids]

    @classmethod
    def paginate_with_count(
        cls,
        table: str,
        page: int = 1,
        per_page: int = 25,
        order_by: Iterable[str] = (),
        exclude_deleted: bool = True,
        **conditions,
    ) -> Tuple[List["DynamicModel"], int]:
        total = cls.count(table, exclude_deleted=exclude_deleted, **conditions)
        items = cls.paginate(
            table, page=page, per_page=per_page, order_by=order_by, exclude_deleted=exclude_deleted, **conditions
        )
        return items, total

    @classmethod
    def first(cls, table: str, exclude_deleted: bool = True, **conditions) -> Optional["DynamicModel"]:
        ids = cls.find_ids(table, order_by=("id",), exclude_deleted=exclude_deleted, limit=1, **conditions)
        return cls(table, ids[0]) if ids else None

    @classmethod
    def last(cls, table: str, exclude_deleted: bool = True, **conditions) -> Optional["DynamicModel"]:
        ids = cls.find_ids(table, order_by=("-id",), exclude_deleted=exclude_deleted, limit=1, **conditions)
        return cls(table, ids[0]) if ids else None

    @classmethod
    def get_by(cls, table: str, exclude_deleted: bool = True, **conditions) -> Optional["DynamicModel"]:
        ids = cls.find_ids(table, exclude_deleted=exclude_deleted, limit=1, **conditions)
        return cls(table, ids[0]) if ids else None

    @classmethod
    def exists_by_id(cls, table: str, row_id: int, exclude_deleted: bool = True) -> bool:
        return cls.exists(table, id=row_id, exclude_deleted=exclude_deleted)

    # -------------------- Aggregate / Count / Exists ----------------------

    @classmethod
    def count(cls, table: str, exclude_deleted: bool = True, **conditions) -> int:
        cond_sql, cond_vals = cls._build_conditions(conditions)
        q = sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(table))
        q, cond_vals = cls._append_soft_delete_filter(table, q, cond_sql, cond_vals, exclude_deleted)

        with cls._get_cursor() as (conn, cur):
            cls._log_sql(conn, q, cond_vals)
            cur.execute(q, cond_vals)
            return cur.fetchone()[0]

    @classmethod
    def exists(cls, table: str, exclude_deleted: bool = True, **conditions) -> bool:
        return cls.count(table, exclude_deleted=exclude_deleted, **conditions) > 0

    @classmethod
    def aggregate(cls, table: str, func: str, column: str, exclude_deleted: bool = True, **conditions) -> Any:
        """
        func: 'SUM' | 'MIN' | 'MAX' | 'AVG' | 'COUNT' | 'COUNT(DISTINCT)'
        column: Spaltenname
        """
        cond_sql, cond_vals = cls._build_conditions(conditions)
        agg_sql = sql.SQL("{}({})").format(sql.SQL(func), sql.Identifier(column))
        q = sql.SQL("SELECT {} FROM {}").format(agg_sql, sql.Identifier(table))
        q, cond_vals = cls._append_soft_delete_filter(table, q, cond_sql, cond_vals, exclude_deleted)
        with cls._get_cursor() as (conn, cur):
            cls._log_sql(conn, q, cond_vals)
            cur.execute(q, cond_vals)
            row = cur.fetchone()
            return row[0] if row else None

    # -------------------- INSERT / UPSERT / BULK --------------------------

    @classmethod
    def create(cls, table: str, column_types: Optional[Dict[str, str]] = None, infer_types: bool = True, **kwargs) -> "DynamicModel":
        """
        Legt einen neuen Datensatz an. Fehlende Spalten werden ergänzt (Typen nach column_types oder inferiert).
        Führt Hooks BEFORE/AFTER Insert aus.
        """
        infos = cls.inspect_schema(table)
        if not infos:
            raise ValueError(f"Tabelle '{table}' existiert nicht.")
        existing = {r["column_name"] for r in infos}

        # Fehlende Spalten anlegen (optional mit Typinferenz)
        for col, val in kwargs.items():
            if col in ("id",):
                continue
            if col not in existing:
                typ = (column_types or {}).get(col) if column_types else None
                if not typ and infer_types:
                    typ = cls._infer_pg_type(val)
                typ = typ or "TEXT"
                stmt = sql.SQL("ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} {}").format(
                    sql.Identifier(table), sql.Identifier(col), sql.SQL(typ)
                )
                with cls._get_cursor() as (conn, cur):
                    cls._log_sql(conn, stmt, None)
                    cur.execute(stmt)
                cls._invalidate_schema_cache(table)
                existing.add(col)

        cls._run_before_hooks(table, kwargs)

        cols = [sql.Identifier(c) for c in kwargs.keys()]
        phs = [sql.Placeholder()] * len(kwargs)
        ins = sql.SQL("INSERT INTO {} ({}) VALUES ({}) RETURNING id").format(
            sql.Identifier(table),
            sql.SQL(", ").join(cols),
            sql.SQL(", ").join(phs),
        )
        vals = list(kwargs.values())
        with cls._get_cursor() as (conn, cur):
            cls._log_sql(conn, ins, vals)
            cur.execute(ins, vals)
            new_id = cur.fetchone()[0]

        cls._run_after_hooks(table, kwargs)
        return cls(table, new_id)

    @classmethod
    def upsert(
        cls,
        table: str,
        conflict_cols: Iterable[str],
        values: Dict[str, Any],
        update_cols: Optional[Iterable[str]] = None,
        column_types: Optional[Dict[str, str]] = None,
        infer_types: bool = True,
    ) -> int:
        """
        INSERT ... ON CONFLICT DO UPDATE.
        Gibt die id zurück (RETURNING id). Erwartet, dass es eine id PK gibt.
        """
        # fehlende Spalten anlegen
        existing = {r["column_name"] for r in cls.inspect_schema(table)}
        for col, val in values.items():
            if col not in existing:
                typ = (column_types or {}).get(col) if column_types else None
                if not typ and infer_types:
                    typ = cls._infer_pg_type(val)
                typ = typ or "TEXT"
                stmt = sql.SQL("ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} {}").format(
                    sql.Identifier(table), sql.Identifier(col), sql.SQL(typ)
                )
                with cls._get_cursor() as (conn, cur):
                    cls._log_sql(conn, stmt, None)
                    cur.execute(stmt)
                cls._invalidate_schema_cache(table)

        cols = [sql.Identifier(c) for c in values.keys()]
        placeholders = [sql.Placeholder()] * len(values)
        conflict = sql.SQL(", ").join(sql.Identifier(c) for c in conflict_cols)

        if update_cols is None:
            update_cols = [c for c in values.keys() if c not in set(conflict_cols) and c != "id"]

        set_exprs = [
            sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(c), sql.Identifier(c)) for c in update_cols
        ]

        stmt = sql.SQL(
            "INSERT INTO {} ({cols}) VALUES ({vals}) ON CONFLICT ({conflict}) DO UPDATE SET {set_exprs} RETURNING id"
        ).format(
            sql.Identifier(table),
            cols=sql.SQL(", ").join(cols),
            vals=sql.SQL(", ").join(placeholders),
            conflict=conflict,
            set_exprs=sql.SQL(", ").join(set_exprs),
        )

        with cls._get_cursor() as (conn, cur):
            vals = list(values.values())
            cls._log_sql(conn, stmt, vals)
            cur.execute(stmt, vals)
            new_id = cur.fetchone()[0]
            return new_id

    @classmethod
    def bulk_create(
        cls,
        table: str,
        rows: List[Dict[str, Any]],
        column_types: Optional[Dict[str, str]] = None,
        infer_types: bool = True,
    ) -> List[int]:
        """
        Fügt mehrere Datensätze in einem großen INSERT ein.
        Fehlende Spalten werden ergänzt.
        """
        if not rows:
            return []
        all_cols = set().union(*(r.keys() for r in rows)) - {"id"}
        existing = {r["column_name"] for r in cls.inspect_schema(table)}
        missing = all_cols - existing

        # fehlende Spalten anlegen mit Typinferenz
        for col in missing:
            typ = (column_types or {}).get(col) if column_types else None
            if not typ and infer_types:
                # Wert aus erster Zeile mit nicht-None
                val = next((r.get(col) for r in rows if r.get(col) is not None), None)
                typ = cls._infer_pg_type(val)
            typ = typ or "TEXT"
            with cls._get_cursor() as (conn, cur):
                stmt = sql.SQL("ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} {}").format(
                    sql.Identifier(table), sql.Identifier(col), sql.SQL(typ)
                )
                cls._log_sql(conn, stmt, None)
                cur.execute(stmt)
        if missing:
            cls._invalidate_schema_cache(table)

        ordered = sorted(all_cols)
        cols_sql = sql.SQL(",").join(map(sql.Identifier, ordered))
        values = [[row.get(c) for c in ordered] for row in rows]

        # BEFORE-Hooks
        for row in rows:
            cls._run_before_hooks(table, row)

        ins = sql.SQL("INSERT INTO {} ({}) VALUES %s RETURNING id").format(sql.Identifier(table), cols_sql)
        with cls._get_cursor() as (conn, cur):
            cls._log_sql(conn, ins, ("<execute_values>",))
            psycopg2.extras.execute_values(cur, ins, values)
            new_ids = [r[0] for r in cur.fetchall()]

        # AFTER-Hooks
        for row in rows:
            cls._run_after_hooks(table, row)
        return new_ids

    @classmethod
    def bulk_update(
        cls,
        table: str,
        rows: List[Dict[str, Any]],
        key: str = "id",
        update_cols: Optional[Iterable[str]] = None,
    ) -> int:
        """
        Batch-Update via UPDATE ... FROM (VALUES ...) alias v(key, col1, ...) WHERE table.key = v.key.
        'rows' müssen den key enthalten.
        Gibt Anzahl betroffener Zeilen zurück.
        """
        if not rows:
            return 0
        if update_cols is None:
            # Alle Keys außer key
            update_cols = sorted(set().union(*(r.keys() for r in rows)) - {key})
        update_cols = list(update_cols)
        if not update_cols:
            return 0

        # VALUES Template
        cols = [key] + update_cols
        values = [[r.get(c) for c in cols] for r in rows]

        v_alias = sql.Identifier("v")
        v_cols = sql.SQL(", ").join(sql.Identifier(c) for c in cols)
        set_exprs = [
            sql.SQL("{t}.{col} = {v}.{col}").format(
                t=sql.Identifier(table),
                col=sql.Identifier(c),
                v=v_alias,
            ) for c in update_cols
        ]

        stmt = sql.SQL(
            "UPDATE {t} SET {set_exprs} FROM (VALUES %s) AS {v} ({vcols}) WHERE {t}.{k} = {v}.{k}"
        ).format(
            t=sql.Identifier(table),
            set_exprs=sql.SQL(", ").join(set_exprs),
            v=v_alias,
            vcols=v_cols,
            k=sql.Identifier(key),
        )

        with cls._get_cursor() as (conn, cur):
            cls._log_sql(conn, stmt, ("<execute_values>",))
            psycopg2.extras.execute_values(cur, stmt, values)
            return cur.rowcount

    @classmethod
    def execute_batch(
        cls, query: str, params: List[Sequence[Any]], page_size: int = 100
    ) -> int:
        """
        Führt execute_batch für parametrisierte Wiederholungen aus.
        Gibt rowcount (letzter batch) zurück.
        """
        with cls._get_cursor() as (conn, cur):
            cls._log_sql(conn, query, ("<execute_batch>",))
            psycopg2.extras.execute_batch(cur, query, params, page_size=page_size)
            return cur.rowcount

    # -------------------- Update/Delete by Conditions ---------------------

    @classmethod
    def update_by_conditions(cls, table: str, updates: Dict[str, Any], **conditions) -> int:
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
        q = sql.SQL("UPDATE {} SET {}").format(sql.Identifier(table), sql.SQL(", ").join(set_parts))
        if cond_sql:
            q += sql.SQL(" WHERE {}").format(cond_sql)
        with cls._get_cursor() as (conn, cur):
            params = set_vals + cond_vals
            cls._log_sql(conn, q, params)
            cur.execute(q, params)
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
        with cls._get_cursor() as (conn, cur):
            cls._log_sql(conn, q, cond_vals)
            cur.execute(q, cond_vals)
            return cur.rowcount

    # -------------------- Soft Delete -------------------------------------

    def soft_delete(self):
        """
        Setzt 'deleted_at' auf NOW() und 'deleted' auf True, statt physisch zu löschen.
        """
        # deleted_at-Feld anlegen falls nicht vorhanden
        if "deleted_at" not in self._columns:
            with self._get_cursor() as (conn, cur):
                stmt = sql.SQL(
                    "ALTER TABLE {t} ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMP"
                ).format(t=sql.Identifier(self._table))
                self._log_sql(conn, stmt, None)
                cur.execute(stmt)
            self._columns.add("deleted_at")
            self.__class__._invalidate_schema_cache(self._table)
        # deleted-Feld anlegen falls nicht vorhanden
        if "deleted" not in self._columns:
            with self._get_cursor() as (conn, cur):
                stmt = sql.SQL(
                    "ALTER TABLE {t} ADD COLUMN IF NOT EXISTS deleted BOOLEAN DEFAULT FALSE"
                ).format(t=sql.Identifier(self._table))
                self._log_sql(conn, stmt, None)
                cur.execute(stmt)
            self._columns.add("deleted")
            self.__class__._invalidate_schema_cache(self._table)

        now = datetime.datetime.utcnow()
        stmt = sql.SQL("UPDATE {t} SET deleted_at = %s, deleted = %s WHERE id = %s").format(
            t=sql.Identifier(self._table)
        )
        with self._get_cursor() as (conn, cur):
            params = (now, True, self._id)
            self._log_sql(conn, stmt, params)
            cur.execute(stmt, params)
        self._data["deleted_at"] = now
        self._data["deleted"] = True

    def restore_soft_deleted(self):
        """
        Setzt 'deleted_at' auf NULL und 'deleted' auf False, um einen Datensatz wiederherzustellen.
        """
        if "deleted_at" not in self._columns or "deleted" not in self._columns:
            return
        stmt = sql.SQL("UPDATE {t} SET deleted_at = NULL, deleted = %s WHERE id = %s").format(
            t=sql.Identifier(self._table)
        )
        with self._get_cursor() as (conn, cur):
            params = (False, self._id)
            self._log_sql(conn, stmt, params)
            cur.execute(stmt, params)
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
            with cls._get_cursor() as (conn, cur):
                stmt = sql.SQL(
                    "ALTER TABLE {t} ADD COLUMN IF NOT EXISTS deleted BOOLEAN DEFAULT FALSE"
                ).format(t=sql.Identifier(table))
                cls._log_sql(conn, stmt, None)
                cur.execute(stmt)
            cls._invalidate_schema_cache(table)
        if "deleted_at" not in columns:
            with cls._get_cursor() as (conn, cur):
                stmt = sql.SQL(
                    "ALTER TABLE {t} ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMP"
                ).format(t=sql.Identifier(table))
                cls._log_sql(conn, stmt, None)
                cur.execute(stmt)
            cls._invalidate_schema_cache(table)

        stmt = sql.SQL(
            "DELETE FROM {t} WHERE deleted = TRUE AND deleted_at IS NOT NULL AND deleted_at < (NOW() - INTERVAL %s MINUTE)"
        ).format(t=sql.Identifier(table))
        with cls._get_cursor() as (conn, cur):
            params = (minutes,)
            cls._log_sql(conn, stmt, params)
            cur.execute(stmt, params)
            return cur.rowcount

    # -------------------- Transaction-Context -----------------------------

    @classmethod
    @contextlib.contextmanager
    def transaction(cls):
        """
        with DynamicModel.transaction():
            … mehrere Operationen …
        commit/rollback automatisch; unterstützt Verschachtelungen via Savepoints.
        """
        # Hole/erzeuge Verbindung
        if cls._connection is None and cls._pool is None:
            raise RuntimeError("Keine Datenbankverbindung.")

        outermost = not cls._in_transaction()
        if outermost:
            # eigene Verbindung reservieren
            conn = cls._current_connection()
            cls._local.conn = conn
            cls._local.depth = 1
        else:
            conn = cls._local.conn  # type: ignore[attr-defined]
            cls._local.depth = getattr(cls._local, "depth", 0) + 1

        savepoint_name = None
        try:
            if not outermost:
                # Nested -> Savepoint
                savepoint_name = f"sp_{int(time.time() * 1000)}_{id(conn)}"
                with conn.cursor() as cur:
                    cur.execute(sql.SQL("SAVEPOINT {}").format(sql.Identifier(savepoint_name)))
            yield
            if outermost:
                conn.commit()
            else:
                with conn.cursor() as cur:
                    cur.execute(sql.SQL("RELEASE SAVEPOINT {}").format(sql.Identifier(savepoint_name)))
        except Exception:
            if outermost:
                conn.rollback()
            else:
                with conn.cursor() as cur:
                    cur.execute(sql.SQL("ROLLBACK TO SAVEPOINT {}").format(sql.Identifier(savepoint_name)))
            raise
        finally:
            if outermost:
                # Verbindung zurückgeben
                try:
                    pass
                finally:
                    if cls._pool:
                        cls._release_connection(conn)
                    cls._local.conn = None
                    cls._local.depth = 0
            else:
                cls._local.depth = max(0, getattr(cls._local, "depth", 1) - 1)

    @classmethod
    @contextlib.contextmanager
    def savepoint(cls, name: Optional[str] = None):
        """
        Separater Savepoint-Context — muss innerhalb von transaction() aufgerufen werden.
        """
        if not cls._in_transaction():
            raise RuntimeError("savepoint() muss innerhalb transaction() verwendet werden.")
        conn = cls._local.conn  # type: ignore[attr-defined]
        sp = name or f"sp_{int(time.time() * 1000)}"
        try:
            with conn.cursor() as cur:
                cur.execute(sql.SQL("SAVEPOINT {}").format(sql.Identifier(sp)))
            yield
            with conn.cursor() as cur:
                cur.execute(sql.SQL("RELEASE SAVEPOINT {}").format(sql.Identifier(sp)))
        except Exception:
            with conn.cursor() as cur:
                cur.execute(sql.SQL("ROLLBACK TO SAVEPOINT {}").format(sql.Identifier(sp)))
            raise

    @classmethod
    def healthcheck(cls) -> bool:
        try:
            with cls._get_cursor() as (conn, cur):
                query = "SELECT 1"
                cls._log_sql(conn, query, None)
                cur.execute(query)
                cur.fetchone()
            return True
        except Exception:
            return False

    # -------------------- Raw / Streaming / Explain -----------------------

    @classmethod
    def raw_query(cls, query: str, params: Iterable[Any] = ()) -> List[Dict[str, Any]]:
        """
        Führt beliebiges SQL aus, gibt List[Dict] zurück (oder [] ohne Ergebnis).
        """
        with cls._get_cursor(dict_cursor=True) as (conn, cur):
            cls._log_sql(conn, query, params)
            cur.execute(query, list(params))
            try:
                return list(cur.fetchall())
            except psycopg2.ProgrammingError:
                return []

    @classmethod
    def stream_query(cls, query: str, params: Iterable[Any] = (), fetch_size: int = 1000) -> Iterator[Dict[str, Any]]:
        """
        Server-seitiger Cursor (Named Cursor) für große Resultsets.
        Achtung: Nicht in aktiver outer transaction committen, bis Streaming fertig ist.
        """
        if cls._connection is None and cls._pool is None:
            raise RuntimeError("Keine Datenbankverbindung.")
        conn = cls._current_connection()
        name = f"ssc_{int(time.time()*1000)}"
        cur = conn.cursor(name=name, cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            cls._log_sql(conn, query, params)
            cur.execute(query, list(params))
            while True:
                rows = cur.fetchmany(fetch_size)
                if not rows:
                    break
                for r in rows:
                    yield dict(r)
            if not cls._in_transaction():
                conn.commit()
        except Exception:
            if not cls._in_transaction():
                conn.rollback()
            raise
        finally:
            try:
                cur.close()
            finally:
                if not cls._in_transaction():
                    cls._release_connection(conn)

    @classmethod
    def explain(cls, query: str, params: Iterable[Any] = (), analyze: bool = True) -> str:
        expl = f"EXPLAIN {'(ANALYZE, BUFFERS)' if analyze else ''} {query}"
        with cls._get_cursor() as (conn, cur):
            cls._log_sql(conn, expl, params)
            cur.execute(expl, list(params))
            lines = [r[0] for r in cur.fetchall()]
            return "\n".join(lines)

    # -------------------- DDL-Operationen ---------------------------------

    @classmethod
    def add_index(cls, table: str, column: str, unique: bool = False):
        idx_name = f"{table}_{column}_{'uniq' if unique else 'idx'}"
        stmt = sql.SQL("CREATE {uniq} INDEX IF NOT EXISTS {iname} ON {t} ({c})").format(
            uniq=sql.SQL("UNIQUE") if unique else sql.SQL(""),
            iname=sql.Identifier(idx_name),
            t=sql.Identifier(table),
            c=sql.Identifier(column),
        )
        with cls._get_cursor() as (conn, cur):
            cls._log_sql(conn, stmt, None)
            cur.execute(stmt)

    @classmethod
    def drop_column(cls, table: str, column: str):
        stmt = sql.SQL("ALTER TABLE {t} DROP COLUMN IF EXISTS {c}").format(
            t=sql.Identifier(table), c=sql.Identifier(column)
        )
        with cls._get_cursor() as (conn, cur):
            cls._log_sql(conn, stmt, None)
            cur.execute(stmt)
        cls._invalidate_schema_cache(table)

    @classmethod
    def rename_column(cls, table: str, old: str, new: str):
        stmt = sql.SQL("ALTER TABLE {t} RENAME COLUMN {o} TO {n}").format(
            t=sql.Identifier(table), o=sql.Identifier(old), n=sql.Identifier(new)
        )
        with cls._get_cursor() as (conn, cur):
            cls._log_sql(conn, stmt, None)
            cur.execute(stmt)
        cls._invalidate_schema_cache(table)

    @classmethod
    def add_unique(cls, table: str, cols: Iterable[str], name: Optional[str] = None):
        cols = list(cols)
        name = name or f"{table}_{'_'.join(cols)}_uniq"
        stmt = sql.SQL("ALTER TABLE {t} ADD CONSTRAINT {n} UNIQUE ({cols})").format(
            t=sql.Identifier(table),
            n=sql.Identifier(name),
            cols=sql.SQL(", ").join(sql.Identifier(c) for c in cols),
        )
        with cls._get_cursor() as (conn, cur):
            cls._log_sql(conn, stmt, None)
            cur.execute(stmt)

    @classmethod
    def drop_constraint(cls, table: str, name: str):
        stmt = sql.SQL("ALTER TABLE {t} DROP CONSTRAINT IF EXISTS {n}").format(
            t=sql.Identifier(table), n=sql.Identifier(name)
        )
        with cls._get_cursor() as (conn, cur):
            cls._log_sql(conn, stmt, None)
            cur.execute(stmt)

    @classmethod
    def add_foreign_key(
        cls,
        table: str,
        column: str,
        ref_table: str,
        ref_column: str = "id",
        on_delete: str = "CASCADE",
        name: Optional[str] = None,
    ):
        name = name or f"{table}_{column}_fk"
        stmt = sql.SQL(
            "ALTER TABLE {t} ADD CONSTRAINT {n} FOREIGN KEY ({c}) REFERENCES {rt} ({rc}) ON DELETE {od}"
        ).format(
            t=sql.Identifier(table),
            n=sql.Identifier(name),
            c=sql.Identifier(column),
            rt=sql.Identifier(ref_table),
            rc=sql.Identifier(ref_column),
            od=sql.SQL(on_delete),
        )
        with cls._get_cursor() as (conn, cur):
            cls._log_sql(conn, stmt, None)
            cur.execute(stmt)

    @classmethod
    def drop_foreign_key(cls, table: str, name: str):
        cls.drop_constraint(table, name)

    @classmethod
    def add_timestamps(cls, table: str):
        """
        Fügt created_at/updated_at hinzu und legt Trigger an, der updated_at bei UPDATE setzt.
        """
        cls.ensure_columns(
            table,
            {
                "created_at": "TIMESTAMPTZ NOT NULL DEFAULT NOW()",
                "updated_at": "TIMESTAMPTZ NOT NULL DEFAULT NOW()",
            },
        )
        func_name = f"{table}_set_updated_at"
        trg_name = f"{table}_trg_set_updated_at"
        create_fn = sql.SQL(
            """
            CREATE OR REPLACE FUNCTION {fn}() RETURNS trigger AS $$
            BEGIN
                NEW.updated_at = NOW();
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
            """
        ).format(fn=sql.Identifier(func_name))
        create_trg = sql.SQL(
            """
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_trigger WHERE tgname = {trg}
                ) THEN
                    CREATE TRIGGER {trg} BEFORE UPDATE ON {t}
                    FOR EACH ROW EXECUTE FUNCTION {fn}();
                END IF;
            END$$;
            """
        ).format(
            trg=sql.Literal(trg_name),
            t=sql.Identifier(table),
            fn=sql.Identifier(func_name),
        )
        with cls._get_cursor() as (conn, cur):
            cls._log_sql(conn, create_fn, None)
            cur.execute(create_fn)
            cls._log_sql(conn, create_trg, None)
            cur.execute(create_trg)

    @classmethod
    def ensure_version_column(cls, table: str, version_col: str = "version"):
        cls.ensure_columns(table, {version_col: "BIGINT NOT NULL DEFAULT 0"})

    # -------------------- Audit Trail -------------------------------------

    @classmethod
    def enable_audit_trail(cls, table: str, audit_table: Optional[str] = None):
        """
        Aktiviert Audit-Log per Trigger für INSERT/UPDATE/DELETE.
        """
        audit_table = audit_table or "audit_log"
        # Audit-Table
        cls.create_table(
            audit_table,
            {
                "table_name": "TEXT NOT NULL",
                "row_id": "BIGINT",
                "action": "TEXT NOT NULL",
                "changed_at": "TIMESTAMPTZ NOT NULL DEFAULT NOW()",
                "before": "JSONB",
                "after": "JSONB",
            },
        )
        func_name = f"audit_{table}_fn"
        trg_name = f"audit_{table}_trg"

        func_sql = sql.SQL(
            """
            CREATE OR REPLACE FUNCTION {fn}() RETURNS trigger AS $$
            BEGIN
                IF TG_OP = 'INSERT' THEN
                    INSERT INTO {at}(table_name, row_id, action, after)
                    VALUES (TG_TABLE_NAME, NEW.id, TG_OP, to_jsonb(NEW));
                    RETURN NEW;
                ELSIF TG_OP = 'UPDATE' THEN
                    INSERT INTO {at}(table_name, row_id, action, before, after)
                    VALUES (TG_TABLE_NAME, NEW.id, TG_OP, to_jsonb(OLD), to_jsonb(NEW));
                    RETURN NEW;
                ELSIF TG_OP = 'DELETE' THEN
                    INSERT INTO {at}(table_name, row_id, action, before)
                    VALUES (TG_TABLE_NAME, OLD.id, TG_OP, to_jsonb(OLD));
                    RETURN OLD;
                END IF;
                RETURN NULL;
            END;
            $$ LANGUAGE plpgsql;
            """
        ).format(fn=sql.Identifier(func_name), at=sql.Identifier(audit_table))

        trg_sql = sql.SQL(
            """
            DO $$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = {trg}) THEN
                    CREATE TRIGGER {trg}
                    AFTER INSERT OR UPDATE OR DELETE ON {t}
                    FOR EACH ROW EXECUTE FUNCTION {fn}();
                END IF;
            END$$;
            """
        ).format(trg=sql.Literal(trg_name), t=sql.Identifier(table), fn=sql.Identifier(func_name))

        with cls._get_cursor() as (conn, cur):
            cls._log_sql(conn, func_sql, None)
            cur.execute(func_sql)
            cls._log_sql(conn, trg_sql, None)
            cur.execute(trg_sql)

    # -------------------- Migration-Framework -----------------------------

    @classmethod
    def add_migration(cls, name: str, fn: Callable[[], None]):
        """
        Registriert eine Migrations-Funktion, die einmalig ausgeführt wird.
        """
        cls._migrations.append((name, fn))

    @classmethod
    def _ensure_migration_table(cls):
        cls.create_table(
            "schema_migrations",
            {
                "name": "TEXT UNIQUE NOT NULL",
                "applied_at": "TIMESTAMPTZ NOT NULL DEFAULT NOW()",
            },
        )
        # name unique ;)
        cls.add_unique("schema_migrations", ["name"], name="schema_migrations_name_key")

    @classmethod
    def run_migrations(cls):
        """
        Führt alle registrierten Migrationen aus, protokolliert in schema_migrations.
        """
        cls._ensure_migration_table()
        with cls._get_cursor(dict_cursor=True) as (conn, cur):
            stmt = "SELECT name FROM schema_migrations"
            cls._log_sql(conn, stmt, None)
            cur.execute(stmt)
            done = {r["name"] for r in cur.fetchall()}

        for name, fn in cls._migrations:
            if name in done:
                continue
            print(f"Running migration: {name}")
            with cls.transaction():
                fn()
                with cls._get_cursor() as (conn, cur):
                    ins = "INSERT INTO schema_migrations (name) VALUES (%s)"
                    cls._log_sql(conn, ins, (name,))
                    cur.execute(ins, (name,))

    # -------------------- Simple Relationships ----------------------------

    def children(self, child_table: str, fk_column: str, exclude_deleted: bool = True) -> List["DynamicModel"]:
        ids = DynamicModel.find_ids(child_table, exclude_deleted=exclude_deleted, **{fk_column: self._id})
        return [DynamicModel(child_table, i) for i in ids]

    def has_many(self, child_table: str, fk_column: str, exclude_deleted: bool = True) -> List["DynamicModel"]:
        return self.children(child_table, fk_column=fk_column, exclude_deleted=exclude_deleted)

    def has_one(self, child_table: str, fk_column: str, exclude_deleted: bool = True) -> Optional["DynamicModel"]:
        return DynamicModel.get_by(child_table, exclude_deleted=exclude_deleted, **{fk_column: self._id})

    def belongs_to(self, parent_table: str, fk_column: str = "", exclude_deleted: bool = True) -> Optional["DynamicModel"]:
        """
        Wenn fk_column leer, wird '<parent_table>_id' erwartet.
        """
        if not fk_column:
            fk_column = f"{parent_table}_id"
        fk_val = getattr(self, fk_column, None)
        if fk_val is None:
            return None
        return DynamicModel.get_by(parent_table, id=fk_val, exclude_deleted=exclude_deleted)

    # -------------------- Instanz-Logik -----------------------------------

    def __init__(self, table: str, row_id: int):
        if self._connection is None and self._pool is None:
            raise RuntimeError("Bitte erst DynamicModel.connect() aufrufen.")
        self._table = table
        self._id = row_id
        self._columns: Set[str] = set()
        self._data: Dict[str, Any] = {}
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
            cols=cols_sql, t=sql.Identifier(self._table)
        )
        with self._get_cursor(dict_cursor=True) as (conn, cur):
            params = (self._id,)
            self._log_sql(conn, q, params)
            cur.execute(q, params)
            row = cur.fetchone()
            if not row:
                raise ValueError(f"Kein Datensatz mit id={self._id} in Tabelle {self._table}.")
            self._data = dict(row)

    def __getattr__(self, name: str) -> Any:
        if name in self._columns:
            return self._data.get(name)
        raise AttributeError(f"'{type(self).__name__}' hat kein Attribut '{name}'")

    def __setattr__(self, name: str, value: Any):
        # interne Felder normal setzen
        if name in (
            "_table",
            "_id",
            "_columns",
            "_data",
            "_connection",
            "_pool",
            "_before_hooks",
            "_after_hooks",
            "_migrations",
            "_logger",
            "_local",
            "_schema_cache",
            "_schema_cache_ttl_seconds",
        ):
            return super().__setattr__(name, value)

        # Update existierende Spalte
        if "_columns" in self.__dict__ and name in self._columns:
            stmt = sql.SQL("UPDATE {t} SET {c} = %s WHERE id = %s").format(
                t=sql.Identifier(self._table), c=sql.Identifier(name)
            )
            with self._get_cursor() as (conn, cur):
                params = (value, self._id)
                self._log_sql(conn, stmt, params)
                cur.execute(stmt, params)
            self._data[name] = value
            return

        # Neue Spalte hinzufügen (mit Typ-Inferenz)
        if "_columns" in self.__dict__:
            typ = self._infer_pg_type(value)
            with self._get_cursor() as (conn, cur):
                add = sql.SQL("ALTER TABLE {t} ADD COLUMN IF NOT EXISTS {c} {typ}").format(
                    t=sql.Identifier(self._table), c=sql.Identifier(name), typ=sql.SQL(typ)
                )
                self._log_sql(conn, add, None)
                cur.execute(add)
            self._columns.add(name)
            self.__class__._invalidate_schema_cache(self._table)

            stmt = sql.SQL("UPDATE {t} SET {c} = %s WHERE id = %s").format(
                t=sql.Identifier(self._table), c=sql.Identifier(name)
            )
            with self._get_cursor() as (conn, cur):
                params = (value, self._id)
                self._log_sql(conn, stmt, params)
                cur.execute(stmt, params)
            self._data[name] = value
            return

        # Fallback
        return super().__setattr__(name, value)

    def save(self):
        """
        Speichert alle im _data gehaltenen Werte (außer id).
        """
        cols = [c for c in self._columns if c != "id"]
        if not cols:
            return
        parts = [sql.SQL("{} = %s").format(sql.Identifier(c)) for c in cols]
        stmt = sql.SQL("UPDATE {t} SET {sets} WHERE id = %s").format(
            t=sql.Identifier(self._table), sets=sql.SQL(", ").join(parts)
        )
        vals = [self._data[c] for c in cols] + [self._id]
        with self._get_cursor() as (conn, cur):
            self._log_sql(conn, stmt, vals)
            cur.execute(stmt, vals)

    def save_with_version(self, version_col: str = "version") -> bool:
        """
        Optimistic Locking: setzt version = version + 1, nur wenn aktuelle Version noch passt.
        Return: True bei Erfolg, False bei Konflikt.
        """
        if version_col not in self._columns:
            raise ValueError(f"Version-Spalte '{version_col}' existiert nicht — nutze ensure_version_column().")
        current_version = self._data.get(version_col, 0)
        cols = [c for c in self._columns if c not in ("id", version_col)]
        set_parts = [sql.SQL("{} = %s").format(sql.Identifier(c)) for c in cols]
        set_parts.append(sql.SQL("{} = {} + 1").format(sql.Identifier(version_col), sql.Identifier(version_col)))
        stmt = sql.SQL("UPDATE {t} SET {sets} WHERE id = %s AND {v} = %s").format(
            t=sql.Identifier(self._table), sets=sql.SQL(", ").join(set_parts), v=sql.Identifier(version_col)
        )
        vals = [self._data[c] for c in cols] + [self._id, current_version]
        with self._get_cursor() as (conn, cur):
            self._log_sql(conn, stmt, vals)
            cur.execute(stmt, vals)
            updated = cur.rowcount == 1
        if updated:
            self._data[version_col] = current_version + 1
        return updated

    def delete(self):
        """
        Physisches Löschen dieses Datensatzes.
        """
        stmt = sql.SQL("DELETE FROM {t} WHERE id = %s").format(t=sql.Identifier(self._table))
        with self._get_cursor() as (conn, cur):
            params = (self._id,)
            self._log_sql(conn, stmt, params)
            cur.execute(stmt, params)

    # -------------------- Convenience: get_or_create ----------------------

    @classmethod
    def get_or_create(
        cls,
        table: str,
        defaults: Optional[Dict[str, Any]] = None,
        exclude_deleted: bool = True,
        **conditions,
    ) -> Tuple["DynamicModel", bool]:
        """
        Atomar: sucht Datensatz mit conditions, erstellt sonst mit defaults+conditions.
        """
        defaults = defaults or {}
        with cls.transaction():
            obj = cls.get_by(table, exclude_deleted=exclude_deleted, **conditions)
            if obj:
                return obj, False
            data = dict(defaults)
            data.update(conditions)
            obj = cls.create(table, **data)
            return obj, True

    # -------------------- Clone / Copy ------------------------------------

    def to_dict(self) -> Dict[str, Any]:
        return dict(self._data)

    def refresh(self) -> None:
        self._load_data()

    def clone_row(self, overrides: Optional[Dict[str, Any]] = None) -> "DynamicModel":
        """
        Klont diesen Datensatz innerhalb derselben Tabelle (ohne id) und gibt die neue Instanz zurück.
        """
        data = self.to_dict()
        data.pop("id", None)
        if overrides:
            data.update(overrides)
        return self.__class__.create(self._table, **data)

    def copy_row_to_table(self, target_table: str, overrides: Optional[Dict[str, Any]] = None) -> "DynamicModel":
        """
        Kopiert diesen Datensatz in eine andere Tabelle (gleiche Spaltennamen, soweit möglich).
        """
        data = self.to_dict()
        data.pop("id", None)
        if overrides:
            data.update(overrides)
        return self.__class__.create(target_table, **data)

    # -------------------- Maintenance -------------------------------------

    @classmethod
    def vacuum_analyze(cls, table: Optional[str] = None):
        stmt = "VACUUM ANALYZE" + (f" {table}" if table else "")
        with cls._get_cursor() as (conn, cur):
            cls._log_sql(conn, stmt, None)
            cur.execute(stmt)
