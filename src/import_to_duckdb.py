import duckdb
from pathlib import Path
import sys

class DuckDBImporter:

    def __init__(self, db_path: str):

        try:
            self.db_path = db_path

            self.conn = duckdb.connect(database=db_path)
            print(f"Connected to DuckDB at '{db_path}'")
            self._initialize_database()
        except Exception as e:
            print(f"Failed to connect or initialize database: {e}", file=sys.stderr)
            sys.exit(1)

    def _initialize_database(self):

        try:
            data_dir = Path(__file__).resolve().parent.parent / "data" / "raw"
            parquet_files = sorted(data_dir.glob("*.parquet"))
            if not parquet_files:
                raise FileNotFoundError(f"No parquet files found in {data_dir}")

            sample_file = parquet_files[0]
            self.conn.execute(f"""
                CREATE TABLE IF NOT EXISTS yellow_taxi_trips AS 
                SELECT * FROM read_parquet('{sample_file}')
                WHERE 1=0;
            """)
            
            self.conn.execute("""
            CREATE TABLE IF NOT EXISTS import_log (
                file_name VARCHAR PRIMARY KEY,
                import_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                rows_imported BIGINT
            );
            """)
            print("Tables 'yellow_taxi_trips' and 'import_log' initialized.")
        except duckdb.Error as e:
            print(f"Error initializing tables: {e}", file=sys.stderr)

    def is_file_imported(self, filename: str) -> bool:
        try:
            result = self.conn.execute(
                "SELECT COUNT(1) FROM import_log WHERE file_name = ?", 
                [filename]
            ).fetchone()
            return result[0] > 0
        except duckdb.Error as e:
            print(f"Error checking import log for '{filename}': {e}", file=sys.stderr)
            return False

    def import_parquet(self, file_path: Path) -> bool:
        filename = file_path.name
        
        if self.is_file_imported(filename):
            print(f"INFO: File '{filename}' is already imported. Skipping.")
            return True

        print(f"Attempting to import '{filename}'...")
        try:
            self.conn.execute("BEGIN TRANSACTION")
            
            count_before_result = self.conn.execute("SELECT COUNT(1) FROM yellow_taxi_trips").fetchone()
            count_before = count_before_result[0] if count_before_result else 0

            self.conn.execute(
                f"INSERT INTO yellow_taxi_trips SELECT * FROM read_parquet(?)", 
                [str(file_path)]
            )

            count_after_result = self.conn.execute("SELECT COUNT(1) FROM yellow_taxi_trips").fetchone()
            count_after = count_after_result[0] if count_after_result else 0
            
            rows_imported = count_after - count_before

            self.conn.execute(
                "INSERT INTO import_log (file_name, rows_imported) VALUES (?, ?)", 
                [filename, rows_imported]
            )

            self.conn.execute("COMMIT")
            
            print(f"SUCCESS: Imported '{filename}' ({rows_imported} rows).")
            return True

        except duckdb.Error as e:

            print(f"ERROR: Failed to import '{filename}'. Rolling back. Error: {e}", file=sys.stderr)
            self.conn.execute("ROLLBACK")
            return False
        except Exception as e:
            print(f"ERROR: An unexpected error occurred with '{filename}'. Rolling back. Error: {e}", file=sys.stderr)
            self.conn.execute("ROLLBACK")
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
            total_trips = self.conn.execute("SELECT COUNT(1) FROM yellow_taxi_trips").fetchone()[0]
            print(f"Total taxi trips: {total_trips:,}")
            
            total_files = self.conn.execute("SELECT COUNT(1) FROM import_log").fetchone()[0]
            print(f"Total files imported: {total_files}")
            
            date_range = self.conn.execute(
                "SELECT MIN(tpep_pickup_datetime), MAX(tpep_pickup_datetime) FROM yellow_taxi_trips"
            ).fetchone()
            if date_range[0]:
                print(f"Date range (pickup): {date_range[0]} to {date_range[1]}")
            
            db_size = self.conn.execute("PRAGMA database_size;").fetchone()[0]
            print(f"Database size: {db_size}")
            
        except duckdb.Error as e:
            print(f"Error fetching statistics: {e}", file=sys.stderr)
        print("---------------------------")

    def close(self):
        if self.conn:
            self.conn.close()
            print(f"\nConnection to '{self.db_path}' closed.")

if __name__ == "__main__":
    
    BASE_DIR = Path(__file__).resolve().parent.parent
    DB_FILE = BASE_DIR / "taxi_data.duckdb"
    DATA_DIR = BASE_DIR / "data" / "raw"

    DATA_DIR.mkdir(exist_ok=True) 

    importer = None
    try:
        importer = DuckDBImporter(db_path=str(DB_FILE))

        importer.import_all_parquet_files(data_dir=DATA_DIR)

        importer.get_statistics()
        
    except Exception as e:
        print(f"An error occurred during the main process: {e}", file=sys.stderr)
    finally:
        if importer:
            importer.close()
