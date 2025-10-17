import requests
from pathlib import Path
from datetime import datetime

class NYCTaxiDataDownloader:
    def __init__(self):
        self.BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
        self.YEAR = 2025
        self.DATA_DIR = Path("data/raw")
        self.DATA_DIR.mkdir(parents=True, exist_ok=True)

    def get_file_path(self, month: int) -> Path:
        filename = f"yellow_tripdata_{self.YEAR}-{month:02d}.parquet"
        return self.DATA_DIR / filename

    def file_exists(self, month: int) -> bool:
        return self.get_file_path(month).exists()

    def download_month(self, month: int) -> bool:
        file_path = self.get_file_path(month)
        if self.file_exists(month):
            print(f"{file_path.name} existe déjà. Ignoré !")
            return True

        url = f"{self.BASE_URL}/{file_path.name}"
        print(f"Téléchargement de {file_path.name}...")

        try:
            with requests.get(url, stream=True) as r:
                r.raise_for_status()
                with open(file_path, 'wb') as f:
                    for chunk in r.iter_content():
                        if chunk:
                            f.write(chunk)
            print("Téléchargement terminé ")
            return True
        except requests.exceptions.RequestException as e:
            print(f"Erreur téléchargement de {file_path.name}: {e}")
            if file_path.exists():
                file_path.unlink()
            return False

    def download_all_available(self) -> list:
        current_month = 12
        downloaded_files = []

        for month in range(1, current_month + 1):
            if self.download_month(month):
                downloaded_files.append(self.get_file_path(month).name)
        print("Fichiers téléchargés :")

        for f in downloaded_files:
            print(f" - {f}")

        return downloaded_files



if __name__ == "__main__":
    downloader = NYCTaxiDataDownloader()
    downloader.download_all_available()
