from __future__ import print_function
from pyspark.sql import SparkSession
from userinfo.Users import Users
from dbconnect.PostgresConnector import PostgresConnector
from load.readCSV import readCSV
from pyspark.sql.types import StructType, StructField, IntegerType, ShortType, StringType, DecimalType, TimestampType 
import sys

class follower(object):
    def __init__(self):
	pass
 	# Process local files 	
    def process_local(self,spark,follower_filepath ):
        df_follower=readCSV().read_localcsv_noschema(spark,follower_filepath)
	self.createmap(spark,df_follower)
    def process_s3(self,spark,follower_filepath,bucketname ):
        df_follower=readCSV().read_s3file(spark,follower_filepath,bucketname)
        self.createmap(spark,df_follower)	
	
    # This code counts the number of projects owned in each language by the owner
    def createmap(self,spark, df_follower):
    	#df_follower.printSchema()
   	df_follower=df_follower.drop(df_follower._c2) # created_at	
    	df_follower=self.cast_follower(spark, df_follower)
	df_res=df_follower.groupby(df_follower.user_id).count()
	df_res.printSchema()
   	df_res.show(10) #TODO Put into DATABASE
	df_user=Users().get_user(spark)
	#df1.join(df2, $"df1Key" === $"df2Key", "inner")
	print ('before join')
	df_user=df_user.join(df_res,df_user.id==df_res.user_id, "left_outer").drop(df_res.user_id)
	print ('After join')
	df_user.show(100)
	self.store_follower(df_user)
	return df_res

    def dftable_query(self,spark,df,query,tablename):
        df.registerTempTable(tablename)
        df=spark.sql(query)
        df.show(15)
        df.printSchema()
        return df

    def cast_follower(self,spark, df_follower):
        df_follower=df_follower.select(
                df_follower._c0.cast("int"),      # id
                df_follower._c1.cast("int")      #user_id
                )
        tablename="followertable"
        query ="SELECT _c0 AS id, _c1 AS user_id from "+tablename
        df2=self.dftable_query(spark, df_follower,query, tablename)
        return df2
    def store_follower(self,  df_follower):
        PostgresConnector().write(df_follower,'follower','overwrite')
        print ('follower stored to DB')

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: count followers <followers.csv> ", file=sys.stderr)
        sys.exit(-1)	
    scSpark = SparkSession \
        .builder \
        .appName("counting issues") \
        .getOrCreate()
    data_file = sys.argv[1] #'/Development/PetProjects/LearningSpark/data.csv'
    #detail_file=sys.argv[2]

    df_follower = scSpark.read.csv(data_file, header=True,  sep=",").cache()
    follower().createmap(df_follower);
   
