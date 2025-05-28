# DynamicModel

`DynamicModel` ist eine kleine Python-ORM-ähnliche Hilfsklasse für PostgreSQL (via `psycopg2`), mit der du

- eine Verbindung aufbauen  
- Tabellen dynamisch anlegen  
- neue Datensätze erstellen (Spalten werden bei Bedarf automatisch als `TEXT` ergänzt)  
- anhand von Spaltenwerten alle passenden `id`s finden  
- dynamisch auf Spalten zugreifen und neue Spalten per Attributszuweisung erstellen können  

## Inhalt

1. [Installation](#installation)  
2. [Verbindung aufbauen](#verbindung-aufbauen)  
3. [Tabelle anlegen](#tabelle-anlegen)  
4. [Datensatz erstellen](#datensatz-erstellen)  
5. [IDs finden](#ids-finden)  
6. [Mit Datensätzen arbeiten](#mit-datensätzen-arbeiten)  
7. [API-Reference](#api-reference)  

---

## Installation

Stelle sicher, dass du `psycopg2` installiert hast:

```bash
pip install psycopg2
```

Dann importiere die Klasse:

```python
from dynamic_model import DynamicModel
```

*(oder füge den Code direkt in dein Projekt ein)*

---

## Verbindung aufbauen

Bevor du irgendetwas mit der Datenbank machst, musst du die Verbindung konfigurieren:

```python
DynamicModel.connect(
    dbname="meine_db",
    user="postgres",
    password="geheim",
    host="localhost",
    port=5432
)
```

Ab jetzt steht `DynamicModel._connection` allen Methoden zur Verfügung.

---

## Tabelle anlegen

Eine neue Tabelle erzeugst du mit einem Namenspaar und einem Schema-Dictionary:

```python
DynamicModel.create_table(
    "users",
    {
      "username": "TEXT NOT NULL UNIQUE",
      "email":    "TEXT",
      "active":   "BOOLEAN DEFAULT TRUE"
    }
)
```

Intern wird automatisch eine Spalte  
```sql
id SERIAL PRIMARY KEY
```  
hinzugefügt.

---

## Datensatz erstellen

Um einen neuen Datensatz einzufügen:

```python
user = DynamicModel.create(
    "users",
    username="homer",
    email="homer@springfield.com",
    active="true"
)

print("Neue ID:", user._id)         # z.B. 1
print("Username:", user.username)   # "homer"
```

Fehlende Spalten im Schema werden automatisch als `TEXT` hinzugefügt.

---

## IDs finden

Mit `find_ids()` kannst du per AND-Verknüpfung nach IDs suchen:

```python
# alle aktiven User
ids_active = DynamicModel.find_ids("users", active="true")
# User mit username='homer' und active='true'
ids_homer = DynamicModel.find_ids("users",
                                  username="homer",
                                  active="true")
```

Die Methode gibt eine Liste von Ganzzahlen zurück.

---

## Mit Datensätzen arbeiten

Nach dem `create()` oder wenn du ein existierendes Objekt lädst…

```python
user = DynamicModel("users", 1)
```

kannst du:

- Ein vorhandenes Feld lesen:  
  ```python
  print(user.email)
  ```
- Ein vorhandenes Feld schreiben (speichert sofort in der DB):  
  ```python
  user.email = "homer@newmail.com"
  ```
- Ein neues Feld per Attribut anlegen (legt automatisch eine `TEXT`-Spalte an und schreibt den Wert):  
  ```python
  user.favourite_donut = "Glazed"
  print(user.favourite_donut)
  ```

Möchtest du mehrere Änderungen gesammelt speichern, kannst du `save()` aufrufen:

```python
user.username = "homer_simpson"
user.active   = "false"
user.save()
```

---

## API Reference

### connect(\*\*db_params)

Richtet die globale DB-Verbindung ein.

Parameter  
- `dbname` (str)  
- `user` (str)  
- `password` (str)  
- `host` (str)  
- `port` (int)

```python
DynamicModel.connect(
    dbname="db",
    user="usr",
    password="pw",
    host="localhost",
    port=5432
)
```

---

### create_table(table: str, schema: dict)

Erzeugt eine neue Tabelle (falls nicht existiert).

- `table`: Tabellenname  
- `schema`: Dict aus Spaltenname → SQL-Typ  

```python
DynamicModel.create_table(
    "products",
    {"name":"TEXT", "price":"NUMERIC"}
)
```

---

### create(table: str, \*\*kwargs) → DynamicModel

Fügt einen Datensatz ein. Fehlende Spalten werden als `TEXT` erstellt.  
Gibt das neue Objekt zurück.

```python
p = DynamicModel.create("products",
                        name="Donut",
                        price="1.50")
```

---

### find_ids(table: str, \*\*conditions) → List[int]

Sucht in `table` alle Zeilen, die **alle** Bedingungen erfüllen.  
Rückgabe: Liste der `id`-Werte.

```python
ids = DynamicModel.find_ids("users",
                            active="true",
                            email="homer@springfield.com")
```

---

### __init__(table: str, row_id: int)

Lädt eine bestehende Zeile (Spalten + Werte) aus der DB.

```python
user = DynamicModel("users", 1)
```

---

### __getattr__(name)

Gibt den Wert der Spalte `name` zurück, falls vorhanden, sonst `AttributeError`.

---

### __setattr__(name, value)

- Wenn `name` eine existierende Spalte ist, wird sofort ein `UPDATE` ausgeführt.  
- Andernfalls wird eine neue `TEXT`-Spalte angelegt und befüllt.

---

### save()

Speichert **alle** Spalten (außer `id`) im Batch. Nützlich, wenn `_data` manuell verändert wurde.

```python
user.username = "bart"
user.email    = "bart@simpsons.com"
# …
user.save()
```

---

## Komplettes Beispiel

```python
from dynamic_model import DynamicModel

# 1) connect
DynamicModel.connect(dbname="demo",
                     user="postgres",
                     password="geheim",
                     host="localhost",
                     port=5432)

# 2) Tabelle anlegen
DynamicModel.create_table("users", {
    "username":"TEXT NOT NULL UNIQUE",
    "email":   "TEXT",
    "active":  "BOOLEAN DEFAULT TRUE"
})

# 3) Datensätze erstellen
DynamicModel.create("users",
                    username="alice",
                    email="alice@example.com",
                    active="true")
DynamicModel.create("users",
                    username="bob",
                    email="bob@example.com",
                    active="false")

# 4) IDs finden
print(DynamicModel.find_ids("users", active="true"))    # z.B. [1]

# 5) Objekt laden & dynamisch arbeiten
u = DynamicModel("users", 1)
print(u.username)          # "alice"
u.nickname = "Allie"       # neue Spalte + Wert
print(u.nickname)          # "Allie"

# 6) Mehr Änderungen & batch-save
u.email   = "alice@new.com"
u.active  = "false"
u.save()
```
