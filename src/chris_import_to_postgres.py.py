# file: loader_duckdb_to_pg.py
from __future__ import annotations

import os
from dataclasses import dataclass
from io import StringIO
from typing import List, Tuple

import duckdb
import pandas as pd
import psycopg2
from psycopg2 import sql

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass


@dataclass
class PostgresConfig:
    host: str = "postgres" # os.getenv("POSTGRES_HOST", os.getenv("PG_HOST", "localhost"))
    port: int = int(os.getenv("POSTGRES_PORT", os.getenv("PG_PORT", "5432")))
    user: str = os.getenv("POSTGRES_USER", os.getenv("PG_USER", "postgres"))
    password: str = os.getenv("POSTGRES_PASSWORD", os.getenv("PG_PASSWORD", "postgres"))
    dbname: str = os.getenv("POSTGRES_DB", os.getenv("PG_DB", "postgres"))
    schema: str = os.getenv("PG_SCHEMA", "public")
    print(">>>>> host:", host, "port:", port, "user:", user, "password:", password, "dbname:", dbname, "schema:", schema)


class SchemaMapper:
    DUCK_TO_PG = {
        "BOOLEAN": "BOOLEAN",
        "TINYINT": "SMALLINT",
        "SMALLINT": "SMALLINT",
        "INTEGER": "INTEGER",
        "BIGINT": "BIGINT",
        "HUGEINT": "NUMERIC(38,0)",
        "REAL": "REAL",
        "FLOAT": "REAL",
        "DOUBLE": "DOUBLE PRECISION",
        "DECIMAL": "NUMERIC",  # width/scale handled upstream if present
        "NUMERIC": "NUMERIC",
        "VARCHAR": "TEXT",
        "BLOB": "BYTEA",
        "DATE": "DATE",
        "TIME": "TIME",
        "TIMESTAMP": "TIMESTAMP",
        "TIMESTAMPTZ": "TIMESTAMPTZ",
        "UUID": "UUID",
    }

    @classmethod
    def to_postgres(cls, duck_type: str) -> str:
        base = duck_type.upper()
        # Handle DECIMAL(p,s) or NUMERIC(p,s)
        if base.startswith("DECIMAL(") or base.startswith("NUMERIC("):
            return base  # already parameterized
        return cls.DUCK_TO_PG.get(base, "TEXT")


class DuckDBReader:
    def __init__(self, duckdb_path: str, read_only: bool = True):
        self.path = duckdb_path
        self.con = duckdb.connect(self.path, read_only=read_only)

    def list_tables(self) -> List[str]:
        rows = self.con.execute(
            """
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_type='BASE TABLE'
            ORDER BY table_schema, table_name
            """
        ).fetchall()
        return [f"{s}.{t}" for s, t in rows]

    def describe_table(self, full_name: str) -> List[Tuple[str, str, str]]:
        schema, table = full_name.split(".", 1)
        return self.con.execute(
            """
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = ? AND table_name = ?
            ORDER BY ordinal_position
            """,
            [schema, table],
        ).fetchall()

    def read_chunk(self, full_name: str, limit: int, offset: int) -> pd.DataFrame:
        schema, table = full_name.split(".", 1)
        # Quote identifiers to preserve case/special chars in DuckDB
        query = f'SELECT * FROM "{schema}"."{table}" LIMIT {int(limit)} OFFSET {int(offset)}'
        return self.con.execute(query).df()  # returns a Pandas DataFrame


class PostgresWriter:
    def __init__(self, cfg: PostgresConfig):
        self.cfg = cfg

    def _connect(self):
        return psycopg2.connect(
            host=self.cfg.host,
            port=self.cfg.port,
            user=self.cfg.user,
            password=self.cfg.password,
            dbname=self.cfg.dbname,
        )

    def ensure_schema(self):
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
                    sql.Identifier(self.cfg.schema)
                )
            )
            conn.commit()

    def create_table_if_not_exists(
        self, table_name: str, columns: List[Tuple[str, str, str]]
    ):
        """
        columns: list of (name, duckdb_type, is_nullable)
        """
        self.ensure_schema()

        col_defs = []
        for name, duck_type, is_nullable in columns:
            pg_type = SchemaMapper.to_postgres(duck_type)
            null_sql = sql.SQL("NULL") if is_nullable.upper() == "YES" else sql.SQL("NOT NULL")
            col_defs.append(
                sql.SQL("{} {} {}").format(
                    sql.Identifier(name),
                    sql.SQL(pg_type),
                    null_sql,
                )
            )

        create_stmt = sql.SQL("CREATE TABLE IF NOT EXISTS {}.{} (") \
            .format(sql.Identifier(self.cfg.schema), sql.Identifier(table_name)) \
            + sql.SQL(", ").join(col_defs) + sql.SQL(")")

        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(create_stmt)
            conn.commit()

    def copy_chunk(self, table_name: str, df: pd.DataFrame):
        if df.empty:
            return

        # Replace NaN with empty string so COPY treats them as NULL with NULL ''
        df = df.where(pd.notnull(df), None)

        buffer = StringIO()
        # Write CSV without header, keep column order
        df.to_csv(buffer, index=False, header=False)
        buffer.seek(0)

        cols_ident = sql.SQL(", ").join([sql.Identifier(c) for c in df.columns])
        copy_sql = sql.SQL(
            "COPY {}.{} ({}) FROM STDIN WITH (FORMAT CSV, HEADER FALSE, NULL '', DELIMITER ',')"
        ).format(sql.Identifier(self.cfg.schema), sql.Identifier(table_name), cols_ident)

        with self._connect() as conn, conn.cursor() as cur:
            cur.copy_expert(copy_sql.as_string(conn), file=buffer)
            conn.commit()


class Loader:
    def __init__(self, duck_path: str, pg_cfg: PostgresConfig):
        self.reader = DuckDBReader(duck_path)
        self.writer = PostgresWriter(pg_cfg)

    def sync_table_schema(self, duck_full: str, pg_table_name: str | None = None):
        if pg_table_name is None:
            pg_table_name = duck_full.split(".", 1)[1]
        cols = self.reader.describe_table(duck_full)
        self.writer.create_table_if_not_exists(pg_table_name, cols)

    def load_table(
        self,
        duck_full: str,
        pg_table_name: str | None = None,
        chunk_size: int = 100_000,
        max_rows: int | None = None,
        verbose: bool = True,
    ):
        if pg_table_name is None:
            pg_table_name = duck_full.split(".", 1)[1]

        # Ensure schema/table exist
        self.sync_table_schema(duck_full, pg_table_name)

        # Stream chunks
        offset = 0
        total = 0
        while True:
            remaining = None if max_rows is None else max(0, max_rows - total)
            lim = chunk_size if remaining is None else min(chunk_size, remaining)
            if lim == 0:
                break

            df = self.reader.read_chunk(duck_full, limit=lim, offset=offset)
            if df.empty:
                if verbose:
                    print(f"Done. Total rows copied: {total}")
                break

            # Preserve column order and names exactly as in DuckDB
            self.writer.copy_chunk(pg_table_name, df)
            n = len(df)
            total += n
            offset += n
            if verbose:
                print(f"Copied {n} rows (total={total})")

            if max_rows is not None and total >= max_rows:
                if verbose:
                    print(f"Reached max_rows={max_rows}. Stopping.")
                break


if __name__ == "__main__":
    # Adapt paths & env as needed
    duckdb_path = "./data/duckdb/data.duckdb"
    pg_cfg = PostgresConfig()

    target_duck_table = "main.my_table"   # <- change if needed
    target_pg_table = "nyctaxi"          # <- or set None to reuse DuckDB table name

    loader = Loader(duckdb_path, pg_cfg)

    print(
        f">>> Using PG {pg_cfg.user}@{pg_cfg.host}:{pg_cfg.port}/{pg_cfg.dbname} schema={pg_cfg.schema}"
    )
    # 1) Create table if not exists (from DuckDB schema)
    loader.sync_table_schema(target_duck_table, target_pg_table)

    # 2) Load data in chunks
    loader.load_table(
        duck_full=target_duck_table,
        pg_table_name=target_pg_table,
        chunk_size=200_000,   # tune per RAM/IO
        max_rows=None,        # or set to a number for tests
        verbose=True,
    )