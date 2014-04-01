DROP TABLE raw_info;

CREATE TABLE raw_info(
   url STRING,
   nom_jvm STRING,
   t_done BIGINT,
   t_resp BIGINT,
   t_page BIGINT
)
STORED AS orc;
