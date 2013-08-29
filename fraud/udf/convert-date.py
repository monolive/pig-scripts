#!/usr/bin/python

from datetime import datetime
import calendar
import time

@outputSchema("date:DateTime")
def date_to_iso_format(date):
  tuple_date = datetime.strptime(date,'%d-%b-%y')
  return tuple_date.strftime('%Y-%m-%d')

@outputSchema("date:int")
def date_to_int(date):
#  return time.mktime(datetime.strptime(date,'%d-%b-%y').timetuple())
  return calendar.timegm(time.strptime(date,'%d-%b-%y'))
