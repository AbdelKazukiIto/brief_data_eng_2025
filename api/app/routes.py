from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from .database import get_db
from .services import TaxiTripService
from .schemas import TaxiTrip, TaxiTripList, TaxiTripCreate, TaxiTripUpdate, Statistics, PipelineResponse
from .download_data import NYCTaxiDataDownloader
from pathlib import Path
from .import_to_postgres import PostgresImporter
from .database import DATABASE_URL

router = APIRouter()

@router.get("/trips", response_model=TaxiTripList, tags=["Trips"])
def get_trips(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    trips, total = TaxiTripService.get_trips(db, skip=skip, limit=limit)
    return {"total": total, "trips": trips}


@router.get("/trips/{trip_id}", response_model=TaxiTrip, tags=["Trips"])
def get_trip(trip_id: int, db: Session = Depends(get_db)):
    trip = TaxiTripService.get_trip(db, trip_id)
    if not trip:
        raise HTTPException(status_code=404, detail="Trip not found")
    return trip


@router.post("/trips", response_model=TaxiTrip, tags=["Trips"])
def create_trip(trip: TaxiTripCreate, db: Session = Depends(get_db)):
    return TaxiTripService.create_trip(db, trip)


@router.put("/trips/{trip_id}", response_model=TaxiTrip, tags=["Trips"])
def update_trip(trip_id: int, trip: TaxiTripUpdate, db: Session = Depends(get_db)):
    updated_trip = TaxiTripService.update_trip(db, trip_id, trip)
    if not updated_trip:
        raise HTTPException(status_code=404, detail="Trip not found")
    return updated_trip


@router.delete("/trips/{trip_id}", response_model=dict, tags=["Trips"])
def delete_trip(trip_id: int, db: Session = Depends(get_db)):
    success = TaxiTripService.delete_trip(db, trip_id)
    if not success:
        raise HTTPException(status_code=404, detail="Trip not found")
    return {"success": success}


@router.get("/statistics", response_model=Statistics, tags=["Statistics"])
def get_statistics(db: Session = Depends(get_db)):
    return TaxiTripService.get_statistics(db)



@router.post("/pipeline/run", response_model=PipelineResponse, tags=["Pipeline"])
def run_pipeline():
    downloader = NYCTaxiDataDownloader()
    downloaded_files = downloader.download_all_available()

    DATA_DIR = Path("data/raw")
    importer = PostgresImporter(DATABASE_URL)
    importer.import_all_parquet_files(DATA_DIR)
    importer.close()

    return {
        "message": "Pipeline executed",
        "downloaded_files": downloaded_files
    }