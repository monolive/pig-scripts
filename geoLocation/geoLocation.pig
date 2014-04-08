-- 
-- Load log file, parse it and store it in HCatalog 
-- 
-- Olivier Renault - orenault@hortonworks.com
-- Use python UDF to extract data from URL
-- Store results in a previously created table 

REGISTER 'akela-0.6-SNAPSHOT.jar';
REGISTER 'geoip-1.2.8.jar';

DEFINE GeoIpLookup com.mozilla.pig.eval.geoip.GeoIpLookup('GeoLiteCity.dat');

RAW_LOGS = LOAD '/user/orenault/weblogs/access_log' as (line:chararray);
LOGS_BASE = FOREACH RAW_LOGS GENERATE 
FLATTEN( 
	REGEX_EXTRACT_ALL(line, '(\\S+) - - \\[([^\\[]+)\\]\\s+"([^"]+)"\\s+(\\d+)\\s+(\\d+)\\s+"([^"]+)"\\s+"([^"]+)"')
)
AS (
	ip: chararray, 
	timestamp: chararray,
        url: chararray,
	status: chararray,
	bytes: chararray,
        referrer: chararray,
	useragent: chararray
);
IP = FOREACH LOGS_BASE GENERATE FLATTEN(ip);
UNIQUE_IP = DISTINCT IP;
GEOIP = FOREACH UNIQUE_IP GENERATE ip, FLATTEN(GeoIpLookup(ip)) AS (country:chararray, country_code:chararray, region:chararray, city:chararray, postal_code:chararray, metro_code:chararray);

-- ILLUSTRATE GEOIP;
-- DESCRIBE GEOIP;
STORE GEOIP INTO 'geoLocation' USING org.apache.hcatalog.pig.HCatStorer();
