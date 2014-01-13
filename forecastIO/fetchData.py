#!/usr/bin/python

from pprint import pprint
import json
import sys
import urllib2
import boto.rds
from lib.config import config
from lib.constants import constants
import dateutil.parser
import MySQLdb

__package__ = "forecastIO.fetchData"

apiKey = config['forecastIO']['apiKey']

db = MySQLdb.connect(
    host=config['db']['host']
  , user=config['db']['username']
  , passwd=config['db']['password']
  , db=config['db']['dbName']
)

dbCursor = db.cursor() 

print('getting location metadata...')
dbCursor.execute("SELECT locationId,latitude,longitude FROM Location WHERE isActive=1;")
locations = []
for row in dbCursor.fetchall():
  locationId = str(row[0])
  latitude = str(row[1])
  longitude = str(row[2])
  location = {
      'locationId': locationId
    , 'latitude': latitude
    , 'longitude': longitude
  }
  locations.append( location )

pprint(locations)

for location in locations:
  pprint(location)
  forecast = forecastio.load_forecast(apiKey, location['latitude'], location['longitude'])

  pprint(forecast)

  #byHour = forecast.hourly()
  #print byHour.summary
  #print byHour.icon

  #currentData = forecast.currently()

