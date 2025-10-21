import pandas as pd
from pathlib import Path

data_dir = Path("data/raw")
first_file = next(data_dir.glob("*.parquet"), None)

if first_file:
    print(f"🔍 Fichier analysé : {first_file.name}")
    df = pd.read_parquet(first_file)
    print("\n--- Colonnes trouvées ---")
    print(df.columns.tolist())
    print(f"\nNombre total de colonnes : {len(df.columns)}")
else:
    print("❌ Aucun fichier .parquet trouvé dans data/raw/")
