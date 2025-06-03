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
        Setzt ein 'deleted_at' Timestamp-Feld auf NOW(), statt physisch zu löschen.
        """
        if "deleted_at" not in self._columns:
            with self._get_cursor() as (_, cur):
                cur.execute(
                    sql.SQL(
                        "ALTER TABLE {t} ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMP"
                    ).format(t=sql.Identifier(self._table))
                )
            self._columns.add("deleted_at")

        now = datetime.datetime.utcnow()
        stmt = sql.SQL("UPDATE {t} SET deleted_at = %s WHERE id = %s").format(
            t=sql.Identifier(self._table)
        )
        with self._get_cursor() as (_, cur):
            cur.execute(stmt, (now, self._id))
        self._data["deleted_at"] = now

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
