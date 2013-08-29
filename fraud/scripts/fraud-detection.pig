/*

Check if two large transaction have been done by the same customer within a 30 days timeframce
Data have been previously uploaded to HCatalog 

*/
--- register UDF to convert date from 03-Jun-12 to 2012-06-03
REGISTER '../udf/convert-date.py' using jython as myfuncs;

--- Read data out of HCatalog
raw = load 'transaction_data' using org.apache.hcatalog.pig.HCatLoader();
large_amount = FILTER raw BY (int)usd_equivalent > 10000; 
large_transaction = FOREACH large_amount GENERATE bkcountry, risktype, loc_id, branch_name, core_client_no, core_ac_no, customer_name, ((DateTime)myfuncs.date_to_iso_format(transaction_date), (chararray)transaction_id) as transaction_record:(date, transaction_id), db_cr, transaction_type, account_ccy, transaction_ccy, conversion_rate, transaction_amount, usd_equivalent;
--- large_transaction = FOREACH large_amount GENERATE bkcountry, risktype, loc_id, branch_name, core_client_no, core_ac_no, customer_name, (DateTime)myfuncs.date_to_iso_format(transaction_date) as date, (chararray)transaction_id as transaction_id, db_cr, transaction_type, account_ccy, transaction_ccy, conversion_rate, transaction_amount, usd_equivalent;

by_user = GROUP large_transaction BY core_client_no;
/*
(200543889,{(US,RR,41,GOUVY,200543889,20110005194,SHYAMAL KUMAR SIKDAR,2009-09-07,S64407378,D,Cash,USD,EUR,1.1345,10000,11345.00),(US,RR,40,LANKLAAR,200543889,20110005194,SHYAMAL KUMAR SIKDAR,2009-09-04,S64046086,D,Cash,USD,EUR,1.2345,10000,12345.00),(US,RR,47,BIEZ,200543889,20110005194,SHYAMAL KUMAR SIKDAR,2009-11-24,S75574228,D,Cash,USD,EUR,1.1345,10000,11345.00)})
*/
--- date_transaction = FOREACH by_user GENERATE $0 as core_client_no, FLATTEN($1.date), FLATTEN($1.transaction_id);
date_transaction = FOREACH by_user GENERATE $0 as core_client_no, FLATTEN($1.transaction_record) as record;
illustrate date_transaction;
--- flat_user = FOREACH date_transaction GENERATE group.$0, date.date, transaction_id.transaction_id;

--- dump date_transaction;
/*
(200543889,US,RR,41,GOUVY,200543889,20110005194,SHYAMAL KUMAR SIKDAR,2009-09-07,S64407378,D,Cash,USD,EUR,1.1345,10000,11345.00)
(200543889,US,RR,40,LANKLAAR,200543889,20110005194,SHYAMAL KUMAR SIKDAR,2009-09-04,S64046086,D,Cash,USD,EUR,1.2345,10000,12345.00)
(200543889,US,RR,47,BIEZ,200543889,20110005194,SHYAMAL KUMAR SIKDAR,2009-11-24,S75574228,D,Cash,USD,EUR,1.1345,10000,11345.00)
*/
--- sorted_by_user = ORDER by_user BY date;
--- illustrate flat_user;
--- dump flat_user;

