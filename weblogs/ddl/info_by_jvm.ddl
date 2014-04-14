DROP TABLE info_by_jvm;

CREATE TABLE info_by_jvm(
   nom_jvm STRING,
   count_jvm BIGINT,
   avg_t_done DOUBLE,
   max_t_done BIGINT,
   min_t_done BIGINT,
   avg_t_resp DOUBLE,
   max_t_resp BIGINT,
   min_t_resp BIGINT,
   avg_t_page DOUBLE,
   max_t_page BIGINT,
   min_t_page BIGINT
)
STORED AS orc;
