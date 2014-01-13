#!/usr/bin/python

from pprint import pprint
import json
import sys
import urllib2
import boto.rds
from lib.config import config
from lib.constants import constants
from datetime import date, timedelta
import dateutil.parser
import MySQLdb

__package__ = "weatherUnderground.fetchData"

apiKey = config['weatherUnderground']['apiKey']
daysToLookBack = config['weatherUnderground']['daysToLookBack']
batchSize = config['db']['defaultBatchSize']

db = MySQLdb.connect(
    host=config['db']['host']
  , user=config['db']['username']
  , passwd=config['db']['password']
  , db=config['db']['dbName']
)

dbCursor = db.cursor() 

#print('getting location metadata...')
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

#pprint(locations)

def buildURL( dateToLookup, latitude, longitude ):

  url = config['weatherUnderground']['apiBaseURL']
  url += apiKey
  url += '/history_'
  url += dateToLookup.strftime('%Y%m%d')
  url += '/q/'
  url += latitude + ',' + longitude
  url += '.json'
  return url


weatherData = []
for location in locations:
  
  today = date.today()

  daysInThePast = 1
  while ( daysInThePast <= daysToLookBack ):

    locationId = location['locationId']
    latitude = location['latitude']
    longitude = location['longitude']

    dateInThePast = today - timedelta(daysInThePast)
    url = buildURL( dateInThePast, latitude, longitude )
    print( url )

    apiResponse = urllib2.urlopen(url)
    apiResponseString = apiResponse.read()
    apiResponseJSON = json.loads(apiResponseString)

    if ( not apiResponseJSON ) \
      or ( not apiResponseJSON['history'] ) \
      or ( not apiResponseJSON['history']['dailysummary'] ) \
      or ( not apiResponseJSON['history']['dailysummary'][0] ):
      break

    dailySummary = apiResponseJSON['history']['dailysummary'][0]

    #pprint(dailySummary)

    try:
      precipitationInches = float(dailySummary['precipi'])
    except ValueError:
      precipitationInches = 0.0

    weatherDatum = {
        'locationId': locationId
      , 'date': dateInThePast.strftime('%Y-%m-%d')
      , 'precipitationInches': precipitationInches
    }
    weatherData.append( weatherDatum )

    daysInThePast = daysInThePast + 1


count = 0

queryStartString = 'INSERT INTO WeatherDataDaily (locationId,date,precipitationInches) VALUES '
queryEndString = ' ON DUPLICATE KEY UPDATE precipitationInches=VALUES(precipitationInches);'

print('upserting into db...')

queryValues = ''
for weatherDatum in weatherData:
  if queryValues != '':
    queryValues += ','
  queryValues += '('
  queryValues += '"' + weatherDatum['locationId'] + '"'
  queryValues += ',"' + weatherDatum['date'] + '"'
  queryValues += ',' + str(weatherDatum['precipitationInches'])
  queryValues += ')'
  count += 1
  if ( ( ( count % batchSize ) == 0 ) or ( count == len(weatherData) ) ):
    query = queryStartString + queryValues + queryEndString
    pprint(query)
    queryValues = ''
    try:
      dbCursor.execute( query )
      db.commit()
    except:
      db.rollback()

print('done!')