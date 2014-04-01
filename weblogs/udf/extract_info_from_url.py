#!/usr/bin/python

import re

@outputSchema("url:tuple(nomPage:chararray, nomJVM:chararray, t_done:long, t_resp:long, t_page:long)")
def extract_url_info(url):
   try:
	nomPage = re.search('nomPage=(.+?)&',url).group(1)
   except AttributeError:
	nomPage = 'blank'
   try:
	nomJVM = re.search('nomJVM=(.+?)&',url).group(1)
   except AttributeError:
	nomJVM = 'blank'
   try:
	t_done = re.search('t_done=(.+?)&',url).group(1)
   except AttributeError:
	t_done = long(0)
   try:
	t_resp = re.search('t_resp=(.+?)&',url).group(1)
   except AttributeError:
	t_resp = long(0)
   try:
	t_page = re.search('t_page=(.+?)&',url).group(1)
   except AttributeError:
	t_page = long(0)
   return nomPage, nomJVM, long(t_done), long(t_resp), long(t_page)
