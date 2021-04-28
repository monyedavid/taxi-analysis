# Project requirements

- 1.4 billion taxi rides (2009-2016)
    - [source](https://academictorrents.com/details/4f465810b86c6b793d1c7556fe3936441081992e)

- gather data insights
- make proposals for business improvements
- suggest one potential approach
- evaluate the impact over the existing data

****

[TaxiZones](src/main/resources/data/taxi_zones.csv)

****

**TaxiDF-Schema**

```text
root
 |-- VendorID: integer (nullable = true) -- unique cabID
 |-- tpep_pickup_datetime: timestamp (nullable = true) -- pickup-timestamp
 |-- tpep_dropoff_datetime: timestamp (nullable = true) -- drop-off-timestamp
 |-- passenger_count: integer (nullable = true) --  (¬_¬)
 |-- trip_distance: double (nullable = true) -- length of trip in miles
 |-- RatecodeID: integer (nullable = true) -- conditions under which taxi ride is going to be paid; 
 |-- store_and_fwd_flag: string (nullable = true)
 |-- PULocationID: integer (nullable = true) -- pickup-location-zone-ID -- [TaxiZonesDF.LocationID]
 |-- DOLocationID: integer (nullable = true) -- drop-off-location-zone-ID
 |-- payment_type: integer (nullable = true) -- credit-card; cash; no-charge; dispute; unknown; voided
 |-- fare_amount: double (nullable = true)
 |-- extra: double (nullable = true)
 |-- mta_tax: double (nullable = true)
 |-- tip_amount: double (nullable = true)
 |-- tolls_amount: double (nullable = true)
 |-- improvement_surcharge: double (nullable = true)
 |-- total_amount: double (nullable = true) -- (¬_¬)
```

****

**TaxiZonesDF-Schema**

```text
root
 |-- LocationID: integer (nullable = true)
 |-- Borough: string (nullable = true) || a town or district which is an administrative unit
 |-- Zone: string (nullable = true)
 |-- service_zone: string (nullable = true)
```

****

**Analytical enquiries**

- which zones have the most pickups/drop-offs | incentives on lower rate zones
- what are the peak hours for taxi | measure demand & exp. with price surges
- how are trips distributed? why are people taking the cab? | segregate market demands & tailor services to them
- what are the peak hours for long/short trips | segregate the services depending on demand for longer/short trips by
  time of day
- top 3 pickups/drop-offs zones for long & short trips
- payment methods for long/short trips
- ride-sharing opportunity by grouping close short trips
- estimate time of day for peak-demands in all zones | approximation of where&when taxis should be available on sight in
  %

[Source](src/main/scala/taxi/Analysis_jan_25_2018.scala)

# Economic impact of grouped taxi rides

- A simple model for estimating potential economic impact over the grouped taxi ride dataset
- The goal is to estimate "economic impart" \\ profits from accepted & rejected group rides using the parameters below

```text
[parameters]
. 5% of taxi trips detected to be group-able @any time
. 30% of people actually accept to be grouped
. 5$ discount on grouped rides
. $2 extra to take an individual ride

. if 2 rides are grouped, reducing cost(maintenance, etc.) by 60% of one average ride
```

[Source](src/main/scala/taxi/EconomicImpact.scala)