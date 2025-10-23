import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

url = os.getenv("DATABASE_URL")
print("DATABASE_URL =", url)

try:
    engine = create_engine(url)
    with engine.connect() as conn:
        print("Connexion établie :", conn.execute(text("SELECT version();")).fetchone())
except Exception as e:
    print("Erreur de connexion :", e)