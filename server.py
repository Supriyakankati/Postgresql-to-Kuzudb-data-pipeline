import os
from datetime import date, datetime
from decimal import Decimal
from sqlalchemy import create_engine, MetaData, select, inspect, text
from sqlalchemy.orm import sessionmaker
from kuzu import Database, Connection

# 0) Configuration
POSTGRES_USER     = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres123")
POSTGRES_HOST     = os.getenv("POSTGRES_HOST", "postgres-db-v1.cbkaui220ofp.us-east-2.rds.amazonaws.com")
POSTGRES_PORT     = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB       = os.getenv("POSTGRES_DB", "call_transcripts")
PG_URL = (
    f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
    f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

KUZU_PATH = os.getenv("KUZU_PATH", "kuzudb_data")
os.makedirs(KUZU_PATH, exist_ok=True)

# 1) Connect to Postgres
engine     = create_engine(PG_URL)
connection = engine.connect()
Session    = sessionmaker(bind=engine)
session    = Session()

print("Postgres test query returned:", connection.execute(text("SELECT 1")).scalar())

# 2) Reflect only the compliance schema
metadata = MetaData()
metadata.reflect(bind=engine, schema="compliance")
print("Reflected tables:", list(metadata.tables.keys()))

# 3) Connect to KùzuDB
db   = Database(KUZU_PATH)
conn = Connection(db)

# Helpers
def infer_type(col):
    from sqlalchemy import Integer, Float, DateTime
    if isinstance(col.type, Integer):  return "INT"
    if isinstance(col.type, Float):    return "DOUBLE"
    if isinstance(col.type, DateTime): return "TIMESTAMP"
    return "STRING"

def format_val(val):
    if val is None:                    return "null"
    if isinstance(val, bool):          return "true" if val else "false"
    if isinstance(val, (int, float, Decimal)): return str(val)
    if isinstance(val, (date, datetime)):     return f"'{val.isoformat()}'"
    s = str(val).replace("'", "\\'")
    return f"'{s}'"

# 4) Bulk-load nodes
print("\nLoading nodes...")
for full_name, tbl in metadata.tables.items():
    name    = tbl.name
    pk_cols = [c.name for c in tbl.columns if c.primary_key]
    if len(pk_cols) != 1:
        print(f"⚠️ Skipping `{name}`: primary key count = {len(pk_cols)}")
        continue

    # DDL
    defs = []
    for col in tbl.columns:
        d = f"{col.name} {infer_type(col)}"
        if col.primary_key:
            d += " PRIMARY KEY"
        defs.append(d)
    conn.execute(f"CREATE NODE TABLE IF NOT EXISTS {name} ({', '.join(defs)});")
    print(f"✅ Created node table: {name}")

    # DML
    rows = session.execute(select(tbl)).fetchall()
    print(f"📥 {len(rows)} rows in `{name}`")
    for row in rows:
        props = ", ".join(f"{col}: {format_val(val)}"
                          for col, val in zip(tbl.columns.keys(), row))
        conn.execute(f"CREATE (n:{name} {{{props}}});")
    print(f"  • Inserted {len(rows)} nodes into `{name}`")

# 5) Bulk-load edges
print("\nLoading relationships...")
inspector = inspect(engine)
for full_name, tbl in metadata.tables.items():
    schema = tbl.schema
    name   = tbl.name
    pk_col = [c.name for c in tbl.columns if c.primary_key][0]
    for fk in inspector.get_foreign_keys(name, schema=schema):
        parent    = fk["referred_table"]
        fk_col    = tbl.columns[fk["constrained_columns"][0]]
        parent_pk = fk["referred_columns"][0]      # the name of the PK col in parent
        child_pk  = list(tbl.primary_key.columns)[0].name

        pairs = session.execute(select(fk_col, tbl.c[child_pk])).fetchall()
        if not pairs:
            continue

        edge = f"{parent}_{name}_edge"
        conn.execute(
            f"CREATE REL TABLE IF NOT EXISTS {edge}"
            f"(FROM {parent} TO {name});"
        )

        for parent_id, child_id in pairs:
            cypher = (
                f"MATCH (a:{parent}), (b:{name}) "
                f"WHERE a.{parent_pk} = {format_val(parent_id)} "
                f"  AND b.{child_pk}  = {format_val(child_id)} "
                f"CREATE (a)-[:{edge}]->(b);"
            )
            conn.execute(cypher)
        print(f"  • Inserted {len(pairs)} relationships for {parent} → {name}")

# 6) Verify
print("\nVerifying graph contents...")
node_tables = conn._get_node_table_names()
rel_meta    = conn._get_rel_table_names()        # list of {"name":…, "src":…, "dst":…}

print("Node tables:", node_tables)
print("Relationship tables (raw):", rel_meta)

# Pull only the edge names
rel_tables = [m["name"] for m in rel_meta]
print("Relationship tables (names):", rel_tables)

print("\nCounts:")
for tbl in node_tables:
    res = conn.execute(f"MATCH (n:{tbl}) RETURN COUNT(n) AS cnt;")
    rec = res.get_next()
    cnt = rec[0] if isinstance(rec, list) else rec.values[0]
    print(f"  • {tbl}: {cnt} nodes")

for rel in rel_tables:
    res = conn.execute(f"MATCH ()-[r:{rel}]->() RETURN COUNT(r) AS cnt;")
    rec = res.get_next()
    cnt = rec[0] if isinstance(rec, list) else rec.values[0]
    print(f"  • {rel}: {cnt} relationships")

# 7) Cleanup
session.close()
connection.close()
conn.close()