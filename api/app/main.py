from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from .database import engine, Base
from .routes import router

Base.metadata.create_all(bind=engine)

app = FastAPI(
    title="NYC Taxi Data Pipeline API",
    description="API REST pour gérer les trajets NYC Taxi et exécuter le pipeline de téléchargement + import",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router, prefix="/api/v1")

@app.get("/", tags=["Root"])
def read_root():
    return {"message": "Bienvenue sur NYC Taxi Data Pipeline API"}

@app.get("/health", tags=["Health"])
def health_check():
    return {"status": "ok"}