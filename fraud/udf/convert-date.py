#!/usr/bin/python

from datetime import datetime

@outputSchema("date:DateTime")
def date_to_iso_format(date):
  tuple_date = datetime.strptime(date,'%d-%b-%y')
  return tuple_date.strftime('%Y-%m-%d')
