-- 
-- Load log file, parse it and store it in HCatalog 
-- 
-- Olivier Renault - orenault@hortonworks.com
-- Use python UDF to extract data from URL
-- Store results in a previously created table 

REGISTER 'akela-0.6-SNAPSHOT.jar';
REGISTER 'geoip-1.2.8.jar';

DEFINE GeoIpLookup com.mozilla.pig.eval.geoip.GeoIpLookup('GeoLiteCity.dat');

RAW_LOGS = LOAD '/user/orenault/weblogs/blancheporte_fr-analytics.3si-btoc.com_access.log-20140301' as (line:chararray);
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

INFO = FOREACH LOGS_BASE GENERATE ip, GeoIpLookup(ip) AS location:tuple(country:chararray, country_code:chararray, region:chararray, city:chararray, postal_code:chararray, metro_code:int);
DUMP INFO
/*
ALL_RESULTS = FOREACH INFO GENERATE url_info.url as url:chararray, url_info.nom_jvm as nom_jvm:chararray, (long)url_info.t_done as t_done, (long)url_info.t_resp as t_resp, (long)url_info.t_page as t_page , ip, useragent;

STORE ALL_RESULTS INTO 'raw_info' USING org.apache.hcatalog.pig.HCatStorer();

RESULTS = FILTER ALL_RESULTS BY url != 'blank';
-- FILTER VALUE of 0 
-- FLTR_RESULTS = FILTER RESULTS BY t_done > 0  and t_resp > 0  and t_page > 0;

GRP_BY_URL = GROUP RESULTS BY url;
RSLT_BY_PAGE = FOREACH GRP_BY_URL GENERATE FLATTEN(RESULTS.url) as url, AVG(RESULTS.t_done) as avg_t_done, MAX(RESULTS.t_done) as max_t_done, MIN(RESULTS.t_done) as min_t_done, AVG(RESULTS.t_resp) as avg_t_resp, MAX(RESULTS.t_resp) as max_t_resp, MIN(RESULTS.t_resp) as min_t_resp, AVG(RESULTS.t_page) as avg_t_page, MAX(RESULTS.t_page) as max_t_page, MIN(RESULTS.t_page) as min_t_page;
STORE RSLT_BY_PAGE INTO 'info_by_page' USING org.apache.hcatalog.pig.HCatStorer();
*/
