/*

Check if two large transaction have been done by the same customer within a 30 days timeframce
Data have been previously uploaded to HCatalog 

Converting the date into int by calculating the amount of days in between date and 01/01/1970

*/
--- register UDF to convert date from 03-Jun-12 to 2012-06-03
REGISTER '../udf/convert-date.py' using jython as myfuncs;

--- Read data out of HCatalog
raw = load 'transaction_data' using org.apache.hcatalog.pig.HCatLoader();
large_amount = FILTER raw BY (int)usd_equivalent > 10000; 
large_transaction = FOREACH large_amount GENERATE core_client_no, customer_name, (loc_id, bkcountry, branch_name, core_ac_no, (chararray)transaction_date, (int)myfuncs.date_to_int(transaction_date), (chararray)transaction_id, db_cr, transaction_type, usd_equivalent) as transaction_record:(loc_id, bkcountry, branch_name, core_ac_no, transaction_date, date, transaction_id, db_cr, transaction_type, usd_equivalent);

by_user = GROUP large_transaction BY (core_client_no, customer_name);
date_transaction = FOREACH by_user GENERATE $0 as customer, FLATTEN($1.transaction_record) as record1, FLATTEN($1.transaction_record) as record2;
--- Check if less than 30 days
minus_duplicate = FILTER date_transaction BY ( record1 != record2 ) AND (record1.date - record2.date < 30 ) AND (record1.date - record2.date > 0 );

--- dump  minus_duplicate;
group_transaction = GROUP minus_duplicate by customer;
all_transaction = FOREACH group_transaction GENERATE $0 as customer, FLATTEN(DIFF($1.record1, $1.record2)) as all_records;

illustrate all_transaction;
