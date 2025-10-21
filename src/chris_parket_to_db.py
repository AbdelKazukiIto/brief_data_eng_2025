import duckdb
import os

# Crée le dossier si nécessaire
os.makedirs("data/duckdb", exist_ok=True)

# Puis crée/ouvre la base
con = duckdb.connect("data/duckdb/data.duckdb",)

# Exemple d'import Parquet → DuckDB
parquet_path = "data/raw/yellow_tripdata_2025-01.parquet"
con.execute("""
CREATE TABLE IF NOT EXISTS my_table AS
SELECT * FROM read_parquet(?)
""", [parquet_path])

# Vérifie le contenu
print(con.sql("DESCRIBE my_table").df())
print(con.sql("SELECT * FROM my_table LIMIT 5").df())

con.close()
