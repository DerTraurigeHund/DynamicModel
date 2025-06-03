# DynamicModel — Ein leichtgewichtiges Mini-ORM

_DynamicModel_ bietet ein flexibles Interface, mit dem Sie Tabellen und Datensätze dynamisch anlegen, anpassen und abfragen können – ganz ohne harte Models. Zusätzlich stehen Features wie Connection-Pooling, Transaktionen, Hooks, Soft-Delete, Bulk-Insert, Paginierung und rudimentäre Migrationen zur Verfügung.

---

## Inhaltsverzeichnis

1. [Installation](#installation)  
2. [Verbindung aufbauen](#verbindung-aufbauen)  
3. [Tabellen-Management](#tabellen-management)  
4. [Hooks (Before/After Insert)](#hooks-beforeafter-insert)  
5. [CRUD-Operationen](#crud-operationen)  
   - [create](#create)  
   - [bulk_create](#bulk_create)  
   - [find_ids / list_all_ids](#find_ids--list_all_ids)  
   - [get_all / paginate](#get_all--paginate)  
   - [update_by_conditions](#update_by_conditions)  
   - [delete_by_conditions](#delete_by_conditions)  
   - [count / exists](#count--exists)  
6. [DDL-Utilities](#ddl-utilities)  
   - [add_index](#add_index)  
   - [drop_column](#drop_column)  
   - [rename_column](#rename_column)  
7. [Beziehungen (Relationships)](#beziehungen-relationships)  
8. [Soft-Delete](#soft-delete)  
9. [Transaktionen](#transaktionen)  
10. [Roh-SQL (raw_query)](#roh-sql-raw_query)  
11. [Rudimentäres Migrations-Framework](#rudimentäres-migrations-framework)  
12. [Instanz-Methoden](#instanz-methoden)  
13. [Beispiele](#beispiele)  

---

## Installation

1. Python ≥ 3.7  
2. `psycopg2` (bzw. `psycopg2-binary`) installieren:  
   ```bash
   pip install psycopg2-binary
   ```
3. `dynamic_model.py` in Ihr Projekt kopieren und importieren:
   ```python
   from dynamic_model import DynamicModel
   ```

---

## Verbindung aufbauen

### Einzelverbindung

```python
DynamicModel.connect(
  dbname="meine_db",
  user="db_user",
  password="secret",
  host="localhost",
  port=5432
)
```

### Connection-Pooling

```python
DynamicModel.connect_pool(
  minconn=1,
  maxconn=10,
  dbname="meine_db",
  user="db_user",
  password="secret",
  host="localhost",
  port=5432
)
```

Intern greifen alle Operationen automatisch auf den Pool oder die Einzelverbindung zu.

---

## Tabellen-Management

### create_table

Erzeugt `CREATE TABLE IF NOT EXISTS …` und fügt automatisch  
`id SERIAL PRIMARY KEY` hinzu.

```python
DynamicModel.create_table(
  "users",
  {
    "username": "TEXT",
    "email":    "TEXT",
    "active":   "BOOLEAN"
  }
)
```

### inspect_schema

Liefert Metadaten aller Spalten einer Tabelle:

```python
schema = DynamicModel.inspect_schema("users")
# Beispiel-Ausgabe:
# [
#   { "column_name": "id",   "data_type": "integer", ... },
#   { "column_name": "username", "data_type": "text", ... },
#   …
# ]
```

---

## Hooks (Before/After Insert)

Sie können Funktionen registrieren, die _vor_ bzw. _nach_ jedem Insert ausgeführt werden:

```python
def stamp_created_at(row):
    import datetime
    row["created_at"] = datetime.datetime.utcnow()

DynamicModel.register_before_insert("users", stamp_created_at)
```

---

## CRUD-Operationen

### create

- Legt einen neuen Datensatz an.  
- Fehlende Spalten werden automatisch als `TEXT` hinzugefügt.  
- Führt vor und nach dem Insert registrierte Hooks aus.  
- Gibt eine `DynamicModel`-Instanz zurück.

```python
user = DynamicModel.create(
  "users",
  username="homer",
  active=True
)
print(user.id, user.username)
```

### bulk_create

Fügt mehrere Zeilen in einem einzigen INSERT ein (via `execute_values`).  
Fehlende Spalten werden ergänzt, Hooks werden pro Zeile ausgelöst.

```python
rows = [
  {"username": "marge", "active": True},
  {"username": "lisa",  "active": False},
]
ids = DynamicModel.bulk_create("users", rows)
```

### find_ids / list_all_ids

- **find_ids**: Sucht IDs, die bestimmten Bedingungen genügen; unterstützt `order_by`.  
- **list_all_ids**: Gibt alle IDs zurück.

```python
ids = DynamicModel.find_ids("users", active=True, order_by=["-id"])
all_ids = DynamicModel.list_all_ids("users")
```

### get_all / paginate

- **get_all**: Liefert alle Datensätze als Instanzen, optional mit `order_by` und Filtern.  
- **paginate**: Paginierung via `LIMIT`/`OFFSET`.

```python
users = DynamicModel.get_all("users", active=True, order_by=["username"])
page1 = DynamicModel.paginate("users", page=1, per_page=10, order_by=["-id"])
```

### update_by_conditions

Führt ein Massen-`UPDATE … SET … WHERE …` durch und gibt Anzahl bearbeiteter Zeilen zurück:

```python
count = DynamicModel.update_by_conditions(
  "users",
  {"active": False},
  username="bart"
)
```

### delete_by_conditions

Löscht Zeilen per Bedingung und liefert die Anzahl gelöschter Reihen:

```python
n = DynamicModel.delete_by_conditions("users", active=False)
```

### count / exists

- **count**: Zählt Zeilen, optional mit Filtern.  
- **exists**: Prüft, ob mindestens eine Zeile existiert.

```python
total = DynamicModel.count("users")
has_smith = DynamicModel.exists("users", username="smith")
```

---

## DDL-Utilities

### add_index

Index (oder Unique-Index) anlegen:

```python
DynamicModel.add_index("users", "email", unique=True)
```

### drop_column

Spalte entfernen, wenn vorhanden:

```python
DynamicModel.drop_column("users", "temp_field")
```

### rename_column

Spalte umbenennen:

```python
DynamicModel.rename_column("users", old="username", new="login")
```

---

## Beziehungen (Relationships)

Im Model selbst können Sie einfache 1-zu-n-Beziehungen abfragen:

```python
user = DynamicModel("users", 42)
orders = user.children("orders", fk_column="user_id")
```

---

## Soft-Delete

Statt zu löschen, setzen Sie einen Zeitstempel in `deleted_at`:

```python
user = DynamicModel("users", 42)
user.soft_delete()
print(user.deleted_at)
```

Bei erstem Einsatz wird automatisch die Spalte `deleted_at TIMESTAMP` angelegt.

---

## Transaktionen

Sammeln Sie mehrere Operationen in einer Transaktion:

```python
with DynamicModel.transaction():
    DynamicModel.create("users", username="test1")
    DynamicModel.create("users", username="test2")
    # bei Fehler: Rollback, sonst Commit
```

---

## Roh-SQL (raw_query)

Führen Sie beliebiges SQL aus und erhalten Sie alle Zeilen als `List[Dict]`:

```python
rows = DynamicModel.raw_query(
  "SELECT username, COUNT(*) AS cnt FROM users GROUP BY username HAVING COUNT(*)>1"
)
```

_Achtung:_ Nur nutzen, wenn nötig (kein automatisches Identifier-Escaping).

---

## Rudimentäres Migrations-Framework

Registrieren Sie Migrations-Funktionen:

```python
def migration_add_flag():
    DynamicModel.add_column("users", "is_admin", "BOOLEAN")

DynamicModel.add_migration("Add is_admin flag", migration_add_flag)
DynamicModel.run_migrations()
```

> Hinweis: In Product­ion empfiehlt sich eine eigene Tabelle zur Versionsverwaltung.

---

## Instanz-Methoden

Nach `__init__(table, id)` stehen Ihnen folgende Methoden zur Verfügung:

| Methode      | Beschreibung                                                |
|--------------|-------------------------------------------------------------|
| `__getattr__`| Greift dynamisch auf Spalten zu (`user.username`).          |
| `__setattr__`| 1) Update einer existierenden Spalte  <br>2) Anlage einer neuen `TEXT`-Spalte |
| `save()`     | Speichert alle geänderten Felder in einem `UPDATE …`        |
| `delete()`   | Physisches Löschen des Datensatzes (`DELETE FROM …`)        |

---

## Beispiele

```python
from dynamic_model import DynamicModel
import datetime

# Verbindung
DynamicModel.connect(dbname="testdb", user="postgres", password="pw")

# Tabelle anlegen
DynamicModel.create_table("users", {
    "username": "TEXT",
    "active":   "BOOLEAN"
})

# Hook: Timestamp vor jedem Insert
def stamp(row):
    row["created_at"] = datetime.datetime.utcnow()
DynamicModel.register_before_insert("users", stamp)

# Einzel-Insert
user = DynamicModel.create("users", username="homer", active=True)
print(user.id, user.username, user.created_at)

# Bulk-Insert
ids = DynamicModel.bulk_create("users", [
    {"username": "marge", "active": True},
    {"username": "lisa",  "active": False},
])
print("Bulk IDs:", ids)

# Abfragen und Pagination
all_users = DynamicModel.get_all("users", active=True, order_by=["username"])
page2 = DynamicModel.paginate("users", page=2, per_page=1)

# Soft-Delete
user = all_users[0]
user.soft_delete()

# Update / Delete by Conditions
DynamicModel.update_by_conditions("users", {"active": False}, username="smith")
DynamicModel.delete_by_conditions("users", active=False)

# DDL-Utilities
DynamicModel.add_index("users", "email", unique=True)
DynamicModel.rename_column("users", "active", "is_active")
DynamicModel.drop_column("users", "temp")

# Roh-SQL
rows = DynamicModel.raw_query("SELECT * FROM users WHERE created_at > %s", [datetime.datetime(2022,1,1)])

# Transaktion
with DynamicModel.transaction():
    DynamicModel.create("users", username="A")
    DynamicModel.create("users", username="B")

# Migration
def mig_add_flag():
    DynamicModel.add_column("users", "flag", "BOOLEAN")
DynamicModel.add_migration("add-flag", mig_add_flag)
DynamicModel.run_migrations()
```
