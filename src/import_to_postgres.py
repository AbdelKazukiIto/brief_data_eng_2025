import os
import sys
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
import inflection

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(dotenv_path=BASE_DIR / ".env")

class PostgresImporter:

    def __init__(self, db_url: str):
        try:
            self.db_url = db_url
            self.engine = create_engine(self.db_url)
            print(f"Connected to PostgreSQL at '{db_url}'")
            self._initialize_database()
        except SQLAlchemyError as e:
            print(f"Failed to connect or initialize database: {e}", file=sys.stderr)
            sys.exit(1)

    def _initialize_database(self):
        try:
            data_dir = Path(__file__).resolve().parent.parent / "data" / "raw"
            parquet_files = sorted(data_dir.glob("*.parquet"))
            if not parquet_files:
                raise FileNotFoundError(f"No parquet files found in {data_dir}")

            sample_file = parquet_files[0]
            df_sample = pd.read_parquet(sample_file)
            df_sample.columns = [inflection.underscore(col) for col in df_sample.columns]

            columns_with_types = ", ".join(
                f"{col} DOUBLE PRECISION" if pd.api.types.is_float_dtype(dtype) else
                f"{col} INT" if pd.api.types.is_integer_dtype(dtype) else
                f"{col} TIMESTAMP" if pd.api.types.is_datetime64_any_dtype(dtype) else
                f"{col} VARCHAR"
                for col, dtype in df_sample.dtypes.items()
            )
            with self.engine.begin() as conn:
                conn.execute(text(f"""
                    CREATE TABLE IF NOT EXISTS yellow_taxi_trips (
                        {columns_with_types}
                    );
                """))
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS import_log (
                        file_name TEXT PRIMARY KEY,
                        import_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        rows_imported BIGINT
                    );
                """))
            print("Tables 'yellow_taxi_trips' and 'import_log' initialized.")
        except Exception as e:
            print(f"Error initializing tables: {e}", file=sys.stderr)

    def is_file_imported(self, filename: str) -> bool:
        try:
            with self.engine.connect() as conn:
                result = conn.execute(
                    text("SELECT 1 FROM import_log WHERE file_name = :file_name"),
                    {"file_name": filename}
                ).first()
            return result is not None
        except SQLAlchemyError as e:
            print(f"Error checking import log for '{filename}': {e}", file=sys.stderr)
            return False

    def import_parquet(self, file_path: Path) -> bool:
        filename = file_path.name

        if self.is_file_imported(filename):
            print(f"INFO: File '{filename}' is already imported. Skipping.")
            return True

        print(f"Attempting to import '{filename}'...")
        try:
            df = pd.read_parquet(file_path)
            df.columns = [inflection.underscore(col) for col in df.columns]

            with self.engine.begin() as conn:
                count_before = conn.execute(text("SELECT COUNT(1) FROM yellow_taxi_trips")).scalar()

                df.to_sql('yellow_taxi_trips', conn, if_exists='append', index=False)

                count_after = conn.execute(text("SELECT COUNT(1) FROM yellow_taxi_trips")).scalar()
                rows_imported = count_after - count_before

                conn.execute(
                    text("INSERT INTO import_log (file_name, rows_imported) VALUES (:file_name, :rows)"),
                    {"file_name": filename, "rows": rows_imported}
                )

            print(f"SUCCESS: Imported '{filename}' ({rows_imported} rows).")
            return True

        except SQLAlchemyError as e:
            print(f"ERROR: Failed to import '{filename}'. Error: {e}", file=sys.stderr)
            return False
        except Exception as e:
            print(f"ERROR: An unexpected error occurred with '{filename}'. Error: {e}", file=sys.stderr)
            return False

    def import_all_parquet_files(self, data_dir: Path) -> int:
        if not data_dir.is_dir():
            print(f"Error: Data directory '{data_dir}' not found.", file=sys.stderr)
            return 0

        print(f"Starting import process for directory: '{data_dir}'")
        parquet_files = list(data_dir.glob("*.parquet"))
        if not parquet_files:
            print("No .parquet files found.")
            return 0

        newly_imported_count = 0
        for file_path in parquet_files:
            was_already_imported = self.is_file_imported(file_path.name)
            success = self.import_parquet(file_path)
            if success and not was_already_imported:
                newly_imported_count += 1

        print(f"\nImport process finished. {newly_imported_count} new file(s) imported.")
        return newly_imported_count

    def get_statistics(self):
        print("\n--- Database Statistics ---")
        try:
            with self.engine.connect() as conn:
                total_trips = conn.execute(text("SELECT COUNT(1) FROM yellow_taxi_trips")).scalar()
                total_files = conn.execute(text("SELECT COUNT(1) FROM import_log")).scalar()
                date_range = conn.execute(
                    text("SELECT MIN(tpep_pickup_datetime), MAX(tpep_pickup_datetime) FROM yellow_taxi_trips")
                ).fetchone()

            print(f"Total taxi trips: {total_trips:,}")
            print(f"Total files imported: {total_files}")
            if date_range[0]:
                print(f"Date range (pickup): {date_range[0]} to {date_range[1]}")
        except SQLAlchemyError as e:
            print(f"Error fetching statistics: {e}", file=sys.stderr)
        print("---------------------------")

    def close(self):
        if self.engine:
            self.engine.dispose()
            print(f"\nConnection to PostgreSQL closed.")


if __name__ == "__main__":
    DATABASE_URL = os.getenv("DATABASE_URL")
    BASE_DIR = Path(__file__).resolve().parent.parent
    DATA_DIR = BASE_DIR / "data" / "raw"
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    importer = None
    try:
        importer = PostgresImporter(DATABASE_URL)
        importer.import_all_parquet_files(DATA_DIR)
        importer.get_statistics()
    except Exception as e:
        print(f"An error occurred during the main process: {e}", file=sys.stderr)
    finally:
        if importer:
            importer.close()
