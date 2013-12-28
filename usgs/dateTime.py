#!/usr/bin/python

from pprint import pprint
#import datetime
#import time
import dateutil.parser

dateTimeString = "2013-12-20T17:30:00.000-08:00"
#dateTimeString = "2013-12-20T17:30:00.000"

#format = "%Y-%m-%dT%H:%M:%S.000"
dt = dateutil.parser.parse(dateTimeString)
pprint(str(dt))