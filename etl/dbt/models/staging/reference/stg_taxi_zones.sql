-- TLC taxi zones (LocationID 1-265). 264 "Unknown" and 265 "Outside of NYC" are real ids that trips use.
select
    "LocationID" as location_id,
    "Borough" as borough,
    "Zone" as zone,
    service_zone
from {{ ref('taxi_zone_lookup') }}
