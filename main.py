import psycopg2
from psycopg2 import sql
import psycopg2.extras

class DynamicModel:
    _connection = None

    @classmethod
    def connect(cls, **db_params):
        """
        Baut eine globale Postgres-Verbindung auf.
        Beispiel:
          DynamicModel.connect(
            dbname="meine_db",
            user="mein_user",
            password="pw",
            host="localhost",
            port=5432
          )
        """
        cls._connection = psycopg2.connect(**db_params)
        cls._connection.autocommit = False

    @classmethod
    def create_table(cls, table: str, schema: dict):
        """
        Legt eine Tabelle an (wenn nicht existiert).
        schema: Dict Spaltenname -> SQL-Typ.
        Fügt automatisch id SERIAL PRIMARY KEY hinzu.
        """
        if cls._connection is None:
            raise RuntimeError("Bitte erst DynamicModel.connect() aufrufen.")
        cols = [ sql.SQL("id SERIAL PRIMARY KEY") ]
        for col, typ in schema.items():
            cols.append(
                sql.SQL("{} {}").format(sql.Identifier(col), sql.SQL(typ))
            )
        q = sql.SQL("CREATE TABLE IF NOT EXISTS {} ({})") \
              .format(sql.Identifier(table), sql.SQL(", ").join(cols))
        with cls._connection.cursor() as cur:
            cur.execute(q)
        cls._connection.commit()

    @classmethod
    def create(cls, table: str, **kwargs):
        """
        Legt einen neuen Datensatz an. Fehlende Spalten werden als TEXT ergänzt.
        Gibt die neue Instanz zurück.
        """
        if cls._connection is None:
            raise RuntimeError("Bitte erst DynamicModel.connect() aufrufen.")

        # vorhandene Spalten ermitteln
        with cls._connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT column_name
                  FROM information_schema.columns
                 WHERE table_schema = 'public'
                   AND table_name   = %s
                """, (table,)
            )
            infos = cur.fetchall()
        if not infos:
            raise ValueError(f"Tabelle '{table}' existiert nicht.")
        existing = {r["column_name"] for r in infos}

        # fehlende Spalten als TEXT anlegen
        with cls._connection.cursor() as cur:
            for col in kwargs:
                if col not in existing:
                    cur.execute(
                        sql.SQL("ALTER TABLE {} ADD COLUMN {} TEXT")
                           .format(sql.Identifier(table), sql.Identifier(col))
                    )
        cls._connection.commit()

        # INSERT bauen
        cols = [sql.Identifier(c) for c in kwargs]
        vals = list(kwargs.values())
        phs  = [sql.Placeholder()]*len(vals)
        ins  = sql.SQL("INSERT INTO {} ({}) VALUES ({}) RETURNING id") \
                   .format(
                     sql.Identifier(table),
                     sql.SQL(", ").join(cols),
                     sql.SQL(", ").join(phs)
                   )
        with cls._connection.cursor() as cur:
            cur.execute(ins, vals)
            new_id = cur.fetchone()[0]
        cls._connection.commit()
        return cls(table, new_id)

    @classmethod
    def find_ids(cls, table: str, **conditions):
        """
        Gibt eine Liste von IDs zurück, deren Spaltenwerte
        (AND-verknüpft) mit den übergebenen Bedingungen übereinstimmen.
        Beispiel:
          ids = DynamicModel.find_ids("users", username="homer", active="true")
        """
        if cls._connection is None:
            raise RuntimeError("Bitte erst DynamicModel.connect() aufrufen.")
        # existierende Spalten prüfen
        with cls._connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT column_name
                  FROM information_schema.columns
                 WHERE table_schema = 'public'
                   AND table_name   = %s
                """, (table,)
            )
            infos = cur.fetchall()
        if not infos:
            raise ValueError(f"Tabelle '{table}' existiert nicht.")
        existing = {r["column_name"] for r in infos}

        # Bedingungen validieren
        for col in conditions:
            if col not in existing:
                raise ValueError(f"Spalte '{col}' existiert nicht in Tabelle '{table}'.")

        # Query bauen
        cond_sql = []
        vals     = []
        for col, val in conditions.items():
            cond_sql.append(
                sql.SQL("{} = %s").format(sql.Identifier(col))
            )
            vals.append(val)

        base = sql.SQL("SELECT id FROM {}").format(sql.Identifier(table))
        if cond_sql:
            base = sql.SQL("{} WHERE {}").format(
                base,
                sql.SQL(" AND ").join(cond_sql)
            )

        with cls._connection.cursor() as cur:
            cur.execute(base, vals)
            rows = cur.fetchall()
        # fetchall liefert Tuples, wir extrahieren das erste Element jeder Zeile
        return [r[0] for r in rows]

    def __init__(self, table: str, row_id: int):
        if self._connection is None:
            raise RuntimeError("Bitte erst DynamicModel.connect() aufrufen.")
        self._table   = table
        self._id      = row_id
        self._columns = set()
        self._data    = {}
        self._load_columns()
        self._load_data()

    def _load_columns(self):
        with self._connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT column_name
                  FROM information_schema.columns
                 WHERE table_schema = 'public'
                   AND table_name   = %s
                """, (self._table,)
            )
            infos = cur.fetchall()
        if not infos:
            raise ValueError(f"Tabelle '{self._table}' existiert nicht.")
        self._columns = {r["column_name"] for r in infos}

    def _load_data(self):
        cols = sql.SQL(", ").join(map(sql.Identifier, self._columns))
        q    = sql.SQL("SELECT {} FROM {} WHERE id = %s") \
                   .format(cols, sql.Identifier(self._table))
        with self._connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(q, (self._id,))
            row = cur.fetchone()
        if not row:
            raise ValueError(f"Kein Datensatz mit id={self._id} in Tabelle {self._table}.")
        self._data = dict(row)

    def __getattr__(self, name):
        if name in self._columns:
            return self._data.get(name)
        raise AttributeError(f"'{type(self).__name__}' hat kein Attribut '{name}'")

    def __setattr__(self, name, value):
        if name in ("_table","_id","_columns","_data","_connection"):
            return super().__setattr__(name, value)

        # Update existierender Spalte
        if name in self._columns:
            q = sql.SQL("UPDATE {} SET {} = %s WHERE id = %s") \
                   .format(sql.Identifier(self._table), sql.Identifier(name))
            with self._connection.cursor() as cur:
                cur.execute(q, (value, self._id))
            self._connection.commit()
            self._data[name] = value
            return

        # Neue Spalte anlegen + befüllen
        with self._connection.cursor() as cur:
            cur.execute(
                sql.SQL("ALTER TABLE {} ADD COLUMN {} TEXT")
                   .format(sql.Identifier(self._table), sql.Identifier(name))
            )
        self._connection.commit()
        self._load_columns()
        q = sql.SQL("UPDATE {} SET {} = %s WHERE id = %s") \
               .format(sql.Identifier(self._table), sql.Identifier(name))
        with self._connection.cursor() as cur:
            cur.execute(q, (value, self._id))
        self._connection.commit()
        self._data[name] = value

    def save(self):
        cols = [c for c in self._columns if c != "id"]
        if not cols:
            return
        set_clause = sql.SQL(", ").join(
            sql.SQL("{} = %s").format(sql.Identifier(c)) for c in cols
        )
        vals = [self._data[c] for c in cols] + [self._id]
        q = sql.SQL("UPDATE {} SET {} WHERE id = %s") \
               .format(sql.Identifier(self._table), set_clause)
        with self._connection.cursor() as cur:
            cur.execute(q, vals)
        self._connection.commit()
