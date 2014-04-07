DROP TABLE geoLocation;

CREATE TABLE geoLocation(
   ip STRING,
   country STRING,
   country_code STRING,
   region STRING,
   city STRING,
   postal_code STRING,
   metro_code STRING
)
STORED AS orc;
