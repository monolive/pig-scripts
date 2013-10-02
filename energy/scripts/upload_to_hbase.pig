raw_data = LOAD 'update2.txt' using PigStorage(',');
copy = STORE raw INTO 'hbase://cust_info' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('info:id info:name info:telephone info:*') AS (info:first_name info:last_name buddies:* info:*);
