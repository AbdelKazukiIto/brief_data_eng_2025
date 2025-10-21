import duckdb
from pathlib import Path
import sys

class DuckDBImporter:
    """
    Gère la connexion et l'importation de fichiers Parquet dans une base DuckDB,
    en évitant les doublons.
    """

    def __init__(self, db_path: str):
        """Initialise la connexion à la base de données et les tables."""
        try:
            self.db_path = db_path
            # Se connecter à la base de données (crée le fichier s'il n'existe pas)
            self.conn = duckdb.connect(database=db_path)
            print(f"Connected to DuckDB at '{db_path}'")
            self._initialize_database()
        except Exception as e:
            print(f"Failed to connect or initialize database: {e}", file=sys.stderr)
            sys.exit(1)

    def _initialize_database(self):
        """Crée les tables nécessaires si elles n'existent pas."""
        try:
            # Schéma de la table principale des trajets
            self.conn.execute("""
                        CREATE TABLE IF NOT EXISTS yellow_taxi_trips (
                            VendorID BIGINT,
                            tpep_pickup_datetime TIMESTAMP,
                            tpep_dropoff_datetime TIMESTAMP,
                            passenger_count DOUBLE,
                            trip_distance DOUBLE,
                            RatecodeID DOUBLE,
                            store_and_fwd_flag VARCHAR,
                            PULocationID BIGINT,
                            DOLocationID BIGINT,
                            payment_type BIGINT,
                            fare_amount DOUBLE,
                            extra DOUBLE,
                            mta_tax DOUBLE,
                            tip_amount DOUBLE,
                            tolls_amount DOUBLE,
                            improvement_surcharge DOUBLE,
                            total_amount DOUBLE,
                            congestion_surcharge DOUBLE,
                            Airport_fee DOUBLE,
                            cbd_congestion_fee DOUBLE
                        );
                        """)

            
            # Schéma de la table de log pour suivre les imports
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
        """Vérifie si un fichier a déjà été importé via la table de log."""
        try:
            result = self.conn.execute(
                "SELECT COUNT(1) FROM import_log WHERE file_name = ?", 
                [filename]
            ).fetchone()
            return result[0] > 0
        except duckdb.Error as e:
            print(f"Error checking import log for '{filename}': {e}", file=sys.stderr)
            return False # Prudence : supposer non importé en cas d'erreur

    def import_parquet(self, file_path: Path) -> bool:
        """
        Importe un unique fichier Parquet, en vérifiant les doublons
        et en utilisant une transaction.
        """
        filename = file_path.name
        
        if self.is_file_imported(filename):
            print(f"INFO: File '{filename}' is already imported. Skipping.")
            return True # Succès (car déjà importé)

        print(f"Attempting to import '{filename}'...")
        try:
            # Utilisation d'une transaction pour garantir l'intégrité
            self.conn.execute("BEGIN TRANSACTION")
            
            # 1. Compter les lignes avant l'import
            count_before_result = self.conn.execute("SELECT COUNT(1) FROM yellow_taxi_trips").fetchone()
            count_before = count_before_result[0] if count_before_result else 0

            # 2. Insérer les données depuis le fichier Parquet
            # DuckDB lit directement le Parquet
            self.conn.execute(
                f"INSERT INTO yellow_taxi_trips SELECT * FROM read_parquet(?)", 
                [str(file_path)]
            )

            # 3. Compter les lignes après l'import
            count_after_result = self.conn.execute("SELECT COUNT(1) FROM yellow_taxi_trips").fetchone()
            count_after = count_after_result[0] if count_after_result else 0
            
            rows_imported = count_after - count_before

            # 4. Enregistrer dans le log
            self.conn.execute(
                "INSERT INTO import_log (file_name, rows_imported) VALUES (?, ?)", 
                [filename, rows_imported]
            )
            
            # 5. Valider la transaction
            self.conn.execute("COMMIT")
            
            print(f"SUCCESS: Imported '{filename}' ({rows_imported} rows).")
            return True

        except duckdb.Error as e:
            # En cas d'erreur, annuler la transaction
            print(f"ERROR: Failed to import '{filename}'. Rolling back. Error: {e}", file=sys.stderr)
            self.conn.execute("ROLLBACK")
            return False
        except Exception as e:
            print(f"ERROR: An unexpected error occurred with '{filename}'. Rolling back. Error: {e}", file=sys.stderr)
            self.conn.execute("ROLLBACK")
            return False

    def import_all_parquet_files(self, data_dir: Path) -> int:
        """
        Parcourt un répertoire et importe tous les fichiers .parquet trouvés.
        Retourne le nombre de *nouveaux* fichiers importés.
        """
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
            # On vérifie avant, pour savoir si l'import (s'il réussit)
            # était un *nouvel* import ou un simple "skip".
            was_already_imported = self.is_file_imported(file_path.name)
            
            success = self.import_parquet(file_path)
            
            if success and not was_already_imported:
                newly_imported_count += 1
                
        print(f"\nImport process finished. {newly_imported_count} new file(s) imported.")
        return newly_imported_count

    def get_statistics(self):
        """Affiche des statistiques sur la base de données."""
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
        """Ferme la connexion à la base de données."""
        if self.conn:
            self.conn.close()
            print(f"\nConnection to '{self.db_path}' closed.")


# --- Point d'entrée du script ---

if __name__ == "__main__":
    
    # Définir les chemins
    BASE_DIR = Path(__file__).resolve().parent
    DB_FILE = BASE_DIR / "taxi_data.duckdb"
    DATA_DIR = BASE_DIR / "data" / "raw"  # Répertoire contenant les .parquet

    # Création d'un répertoire de données factice s'il n'existe pas
    # (Pour les tests, vous devriez placer vos vrais fichiers Parquet ici)
    DATA_DIR.mkdir(exist_ok=True) 

    importer = None
    try:
        importer = DuckDBImporter(db_path=str(DB_FILE))
        
        # Importer tous les fichiers
        importer.import_all_parquet_files(data_dir=DATA_DIR)
        
        # Afficher les statistiques
        importer.get_statistics()
        
    except Exception as e:
        print(f"An error occurred during the main process: {e}", file=sys.stderr)
    finally:
        # S'assurer que la connexion est fermée
        if importer:
            importer.close()
