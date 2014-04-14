-- 
-- Load log file, parse it and store it in HCatalog 
-- 
-- Olivier Renault - orenault@hortonworks.com
-- Use python UDF to extract data from URL
-- Store results in a previously created table 

REGISTER 'udf/extract_info_from_url.py' using jython as myfuncs;

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

INFO = FOREACH LOGS_BASE GENERATE ip, myfuncs.extract_url_info(url) as url_info:(url:chararray, nom_jvm:chararray, t_done:long, t_resp:long, t_page:long);;
ALL_RESULTS = FOREACH INFO GENERATE ip, url_info.url as url:chararray, url_info.nom_jvm as nom_jvm:chararray, (long)url_info.t_done as t_done, (long)url_info.t_resp as t_resp, (long)url_info.t_page as t_page;

STORE ALL_RESULTS INTO 'raw_info' USING org.apache.hcatalog.pig.HCatStorer();

RESULTS = FILTER ALL_RESULTS BY url != 'blank';


-- GET information by URL 
GRP_BY_URL = GROUP RESULTS BY url;
RSLT_BY_PAGE = FOREACH GRP_BY_URL GENERATE FLATTEN(RESULTS.url) as url, AVG(RESULTS.t_done) as avg_t_done, MAX(RESULTS.t_done) as max_t_done, MIN(RESULTS.t_done) as min_t_done, AVG(RESULTS.t_resp) as avg_t_resp, MAX(RESULTS.t_resp) as max_t_resp, MIN(RESULTS.t_resp) as min_t_resp, AVG(RESULTS.t_page) as avg_t_page, MAX(RESULTS.t_page) as max_t_page, MIN(RESULTS.t_page) as min_t_page;
DST_BY_PAGE = DISTINCT RSLT_BY_PAGE;
STORE DST_BY_PAGE INTO 'info_by_page' USING org.apache.hcatalog.pig.HCatStorer();

-- GET information by JVM 
GRP_BY_JVM = GROUP RESULTS BY nom_jvm;
RSLT_BY_JVM = FOREACH GRP_BY_JVM GENERATE FLATTEN(RESULTS.nom_jvm ) as nom_jvm, COUNT(RESULTS.nom_jvm) as count_jvm,  AVG(RESULTS.t_done) as avg_t_done, MAX(RESULTS.t_done) as max_t_done, MIN(RESULTS.t_done) as min_t_done, AVG(RESULTS.t_resp) as avg_t_resp, MAX(RESULTS.t_resp) as max_t_resp, MIN(RESULTS.t_resp) as min_t_resp, AVG(RESULTS.t_page) as avg_t_page, MAX(RESULTS.t_page) as max_t_page, MIN(RESULTS.t_page) as min_t_page;
DST_BY_JVM = DISTINCT RSLT_BY_JVM;
STORE DST_BY_JVM INTO 'info_by_jvm' USING org.apache.hcatalog.pig.HCatStorer();
