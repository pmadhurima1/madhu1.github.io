from configparser import ConfigParser

#Create the Database properties
db_properties={}
config = configparser.ConfigParser()
config.read("db_properties.ini")
db_prop = config['postgres']
db_url = db_prop['url']
db_properties['username']=db_prop['username']
db_properties['password']=db_prop['properties']
db_properties['url']=db_prop['url']
db_properties['driver']=db_prop['driver']

df = spark.read.jdbc(url=url,table='ghtdb.lang_results',properties=db_properties)
