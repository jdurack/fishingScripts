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

__package__ = "usgs.fetchData"

db = MySQLdb.connect(
    host=config['db']['host']
  , user=config['db']['username']
  , passwd=config['db']['password']
  , db=config['db']['dbName']
)

dbCursor = db.cursor() 

dbCursor.execute("SELECT locationId,usgsSiteId FROM Location WHERE isActive=1;")
usgsSiteIdToLocationId = {}
sites = ''
for row in dbCursor.fetchall():
  locationId = str(row[0])
  usgsSiteId = str(row[1])
  if sites != '':
    sites += ','
  sites += usgsSiteId
  usgsSiteIdToLocationId[usgsSiteId] = locationId

dbCursor.execute("SELECT paramId,usgsParameterCd FROM Param WHERE isActive=1;")
usgsParameterCdToParamId = {}
parameterCds = ''
for row in dbCursor.fetchall():
  paramId = str(row[0])
  usgsParameterCd = str(row[1])
  if parameterCds != '':
    parameterCds += ','
  parameterCds += usgsParameterCd
  usgsParameterCdToParamId[usgsParameterCd] = paramId

def buildURL( baseURL, params ):
  url = baseURL
  first = True
  for paramKey,paramValue in params.iteritems():
    if first:
      url += '?'
    else:
      url += '&'
    url += paramKey + '=' + paramValue
    first = False
  return url

apiParams = {}
apiParams['format'] = config['usgs']['fetchFormat']
apiParams['period'] = config['usgs']['defaultFetchPeriod']
apiParams['parameterCd'] = parameterCds
apiParams['sites'] = sites

url = buildURL( config['usgs']['apiBaseURL'], apiParams )
apiResponse = urllib2.urlopen(url)
apiResponseString = apiResponse.read()
apiResponseJSON = json.loads(apiResponseString)

usgsData = []
timeSeriesSets = apiResponseJSON['value']['timeSeries']
for timeSeriesSet in timeSeriesSets:
  sourceInfo = timeSeriesSet['sourceInfo']
  variable = timeSeriesSet['variable']
  values = timeSeriesSet['values']

  usgsSiteId = sourceInfo['siteCode'][0]['value']
  locationId = usgsSiteIdToLocationId[usgsSiteId]

  usgsParameterCd = variable['variableCode'][0]['value']
  paramId = usgsParameterCdToParamId[usgsParameterCd]

  valueSet = values[0]['value']
  for value in valueSet:
    usgsDatum = {}
    usgsDateTime = value['dateTime']
    mysqlFormattedDateTime = dateutil.parser.parse(usgsDateTime).strftime(constants['mysqlDateTimeFormat'])
    usgsDatum['localDateTime'] = mysqlFormattedDateTime
    usgsDatum['value'] = value['value']
    usgsDatum['locationId'] = locationId
    usgsDatum['paramId'] = paramId
    usgsData.append(usgsDatum)

count = 0
batchSize = 100

queryStartString = 'INSERT INTO usgsData (localDateTime,locationId,paramId,value) VALUES '
queryEndString = ' ON DUPLICATE KEY UPDATE value=VALUES(value);'

queryValues = ''
for usgsDatum in usgsData:
  if queryValues != '':
    queryValues += ','
  queryValues += '('
  queryValues += '"' + usgsDatum['localDateTime'] + '"'
  queryValues += ',' + usgsDatum['locationId']
  queryValues += ',' + usgsDatum['paramId']
  queryValues += ',' + usgsDatum['value']
  queryValues += ')'
  count += 100
  if ( ( ( count % batchSize ) == 0 ) or ( count == len(usgsData) ) ):
    query = queryStartString + queryValues + queryEndString
    #pprint(query)
    queryValues = ''
    try:
      dbCursor.execute( query )
      db.commit()
    except:
      db.rollback()

print('Done!')