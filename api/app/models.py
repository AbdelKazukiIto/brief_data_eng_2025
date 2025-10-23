from sqlalchemy import Column, String, BigInteger, TIMESTAMP, Float, Integer
from sqlalchemy.orm import declarative_base
from datetime import datetime

Base = declarative_base()

class YellowTaxiTrip(Base):
    __tablename__ = "yellow_taxi_trips"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    vendor_id = Column(Integer)
    tpep_pickup_datetime = Column(TIMESTAMP)
    tpep_dropoff_datetime = Column(TIMESTAMP)
    passenger_count = Column(Integer)
    trip_distance = Column(Float)
    ratecode_id = Column(Integer)
    store_and_fwd_flag = Column(String)
    pu_location_id = Column(Integer)
    do_location_id = Column(Integer)
    payment_type = Column(Integer)
    fare_amount = Column(Float)
    extra = Column(Float)
    mta_tax = Column(Float)
    tip_amount = Column(Float)
    tolls_amount = Column(Float)
    improvement_surcharge = Column(Float)
    total_amount = Column(Float)
    congestion_surcharge = Column(Float)
    airport_fee = Column(Float)
    cbd_congestion_fee = Column(Float)

class ImportLog(Base):
    __tablename__ = "import_log"

    file_name = Column(String, primary_key=True)
    import_date = Column(TIMESTAMP, default=datetime.now)
    rows_imported = Column(BigInteger)
