#!/usr/bin/python

from datetime import datetime

@outputSchema("date:DateTime")
def date_to_iso_format(date):
  tuple_date = datetime.strptime(date,'%d-%b-%y')
  return tuple_date.strftime('%Y-%m-%d')

@outputSchema("date:int")
def date_to_int(date):
  # return how many days have elpased since the 01/01/1970 
  tuple_date = datetime.strptime(date,'%d-%b-%y')
  reference = datetime.strptime('01-Jan-1970','%d-%b-%y')
  days =  (tuple_date - reference).days
  return days
