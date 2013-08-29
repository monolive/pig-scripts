/*

Check if two large transaction have been done by the same customer within a 30 days timeframce
Data have been previously uploaded to HCatalog 

Converting the date into unix timestamp

*/
--- register UDF to convert date from 03-Jun-12 to 2012-06-03
REGISTER '../udf/convert-date.py' using jython as myfuncs;

--- Read data out of HCatalog
raw = load 'transaction_data' using org.apache.hcatalog.pig.HCatLoader();
large_amount = FILTER raw BY (int)usd_equivalent > 10000; 
large_transaction = FOREACH large_amount GENERATE bkcountry, risktype, loc_id, branch_name, core_client_no, core_ac_no, customer_name, ((int)myfuncs.date_to_int(transaction_date), (chararray)transaction_id) as transaction_record:(date, transaction_id), db_cr, transaction_type, account_ccy, transaction_ccy, conversion_rate, transaction_amount, usd_equivalent;

by_user = GROUP large_transaction BY core_client_no;
date_transaction = FOREACH by_user GENERATE $0 as core_client_no, FLATTEN($1.transaction_record) as record1, FLATTEN($1.transaction_record) as record2;
--- Check if less than 30 days
minus_duplicate = FILTER date_transaction BY ( record1 != record2 ) AND (record1.date - record2.date < 30 ) AND (record1.date - record2.date > 0 );
illustrate  minus_duplicate;
