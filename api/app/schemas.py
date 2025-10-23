from pydantic import BaseModel, ConfigDict
from datetime import datetime
from typing import Optional, List


class TaxiTripBase(BaseModel):
    vendor_id: Optional[int] = None
    tpep_pickup_datetime: Optional[datetime] = None
    tpep_dropoff_datetime: Optional[datetime] = None
    passenger_count: Optional[int] = None
    trip_distance: Optional[float] = None
    ratecode_id: Optional[int] = None
    store_and_fwd_flag: Optional[str] = None
    pu_location_id: Optional[int] = None
    do_location_id: Optional[int] = None
    payment_type: Optional[int] = None
    fare_amount: Optional[float] = None
    extra: Optional[float] = None
    mta_tax: Optional[float] = None
    tip_amount: Optional[float] = None
    tolls_amount: Optional[float] = None
    improvement_surcharge: Optional[float] = None
    total_amount: Optional[float] = None
    congestion_surcharge: Optional[float] = None
    airport_fee: Optional[float] = None
    cbd_congestion_fee: Optional[float] = None

    model_config = ConfigDict(from_attributes=True)


class TaxiTripCreate(TaxiTripBase):
    pass


class TaxiTripUpdate(TaxiTripBase):
    pass


class TaxiTrip(TaxiTripBase):
    id: int
    model_config = ConfigDict(from_attributes=True)


class TaxiTripList(BaseModel):
    total: int
    trips: List[TaxiTrip]


class Statistics(BaseModel):
    total_trips: int
    total_files: int
    first_pickup: Optional[datetime] = None
    last_pickup: Optional[datetime] = None
    average_trip_distance: Optional[float] = None
    average_fare_amount: Optional[float] = None


class PipelineResponse(BaseModel):
    message: str
    files_imported: Optional[List[str]] = None