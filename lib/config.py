import json

configFilename = 'config.json'
configFile = open(configFilename, 'r')

configDataString = configFile.read()
config = json.loads(configDataString)