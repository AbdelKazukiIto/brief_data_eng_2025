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
                Airport_fee DOUBLE
            )

