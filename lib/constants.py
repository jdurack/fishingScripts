import json

constantsFilename = 'constants.json'
constantsFile = open(constantsFilename, 'r')

constantsDataString = constantsFile.read()
constants = json.loads(constantsDataString)