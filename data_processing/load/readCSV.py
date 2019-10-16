from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
class readCSV:
    	
    def read_s3file(self,sc,filename,bucketname):
	sqlcontext = SQLContext(sc)
	df = sqlcontext.read.csv("s3a://"+bucketname+"/"+filename, header=False,inferSchema=True)
	return df
    def read_s3file_filepath(self,sc,filepath, input_schema):
        sqlcontext = SQLContext(sc)
        df = sqlcontext.read.csv(filepath, header=False,schema=input_schema)
        return df

    def read_localcsv(self, spark, filename,dfschema):
	df=spark.read.csv(filename, header=False ,sep=",",schema=dfschema).cache()
	return df	 
    def read_localcsv_noschema(self, spark, filepath):
        df=spark.read.csv(filepath, header=False ,sep=",").cache()
        return df
