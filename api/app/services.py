from sqlalchemy.orm import Session
from sqlalchemy import func
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import text
from .models import YellowTaxiTrip
from .schemas import TaxiTripCreate, TaxiTripUpdate, Statistics


class TaxiTripService:

    @staticmethod
    def get_trip(db: Session, trip_id: int) -> YellowTaxiTrip | None:
        return db.query(YellowTaxiTrip).filter(YellowTaxiTrip.id == trip_id).first()

    @staticmethod
    def get_trips(db: Session, skip: int = 0, limit: int = 100) -> tuple[list[YellowTaxiTrip], int]:
        total = db.query(func.count(YellowTaxiTrip.id)).scalar()
        trips = db.query(YellowTaxiTrip).offset(skip).limit(limit).all()
        return trips, total

    @staticmethod
    def create_trip(db: Session, trip: TaxiTripCreate) -> YellowTaxiTrip:
        new_trip = YellowTaxiTrip(**trip.model_dump())
        db.add(new_trip)
        db.commit()
        db.refresh(new_trip)
        return new_trip

    @staticmethod
    def update_trip(db: Session, trip_id: int, trip: TaxiTripUpdate) -> YellowTaxiTrip | None:
        existing_trip = db.query(YellowTaxiTrip).filter(YellowTaxiTrip.id == trip_id).first()
        if not existing_trip:
            return None
        for key, value in trip.model_dump(exclude_unset=True).items():
            setattr(existing_trip, key, value)
        db.commit()
        db.refresh(existing_trip)
        return existing_trip

    @staticmethod
    def delete_trip(db: Session, trip_id: int) -> bool:
        existing_trip = db.query(YellowTaxiTrip).filter(YellowTaxiTrip.id == trip_id).first()
        if not existing_trip:
            return False
        db.delete(existing_trip)
        db.commit()
        return True

    @staticmethod
    def get_statistics(db: Session) -> Statistics:
        try:
            total_trips = db.query(func.count(YellowTaxiTrip.id)).scalar()
            first_pickup, last_pickup = db.query(
                func.min(YellowTaxiTrip.tpep_pickup_datetime),
                func.max(YellowTaxiTrip.tpep_pickup_datetime)
            ).first()
            avg_distance, avg_fare = db.query(
                func.avg(YellowTaxiTrip.trip_distance),
                func.avg(YellowTaxiTrip.fare_amount)
            ).first()

            return Statistics(
                total_trips=total_trips,
                total_files=0,
                first_pickup=first_pickup,
                last_pickup=last_pickup,
                average_trip_distance=avg_distance,
                average_fare_amount=avg_fare
            )
        except SQLAlchemyError as e:
            print("Erreur get_statistics:", e)
            return Statistics(total_trips=0, total_files=0)