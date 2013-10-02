raw_data = LOAD 'update2.txt' using PigStorage(',');
results = FOREACH raw_data GENERATE (long)$0 as id, (chararray)$1 as username, (long)$3 as telephone, (chararray)$2 as city, (chararray)$4 as status, (chararray)$TIMESTAMP as timestamp;
store results into 'cust_info' using org.apache.hcatalog.pig.HCatStorer();
