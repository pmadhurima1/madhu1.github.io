from dbconnect.PostgresConnector import PostgresConnector
from load.readCSV import readCSV
from pyspark import SparkContext
from pyspark.sql import SparkSession,SQLContext
from pyspark.sql.types import * 
from pyspark.sql.functions import col , column
import sys

class Users(object):
    #id,login, name, created_at, type, fake, deleted, long,lat,country_code,state, city
    user_schema = StructType([
	StructField("id", 	IntegerType(),True),	 	#_C0  
	StructField("login",    StringType(),True), 		#_c1
	StructField("company",  StringType(),True), 		#_c2
	StructField("created_at", TimestampType(),True),	#_c3
	StructField("type", 	StringType(),True),		#_c4
	StructField("fake", 	IntegerType(),True), 		#_c5
	StructField("deleted", 	IntegerType(),True),		#_c6
    	StructField("long", 	DecimalType(),True),		#_c7
    	StructField("lat", 	DecimalType(),True),		#_c8
    	StructField("country_code", StringType(),True),		#_c9
    	StructField("country", 	StringType(), True),		#_c10
	StructField("state",  StringType(),   True),		#_c11
	StructField("city",  StringType(),    True),             #_c12
    	])
# This code counts the number of projects owned in each language by the owner
    def __init__(self):    
	pass

    def store_userdb(self, sdfUsers):
    	print('Total Records = {}'.format(sdfUsers.count()))
   	sdfUsers.show()
	PostgresConnector().write(sdfUsers,'Users','overwrite')
	print ('Users stored to DB')
#'''	
#BinaryType: binary
#BooleanType: boolean
#ByteType: tinyint
#IntegerType: int
#LongType: bigint
#ShortType: smallint
#StringType: string
#TimestampType: timestamp
#'''
    def read_user(self, spark, user_df):
	#user_df.registerTempTable("usertable")
	#user_df=spark.sql("SELECT *  FROM usertable WHERE type = 'USR' AND country_code='us'")
	#user_df=spark.sql("SELECT *  FROM usertable WHERE usertable._c4 = 'USR' AND usertable._c9='us'")
	user_df.show(15)
	user_df.printSchema()	
	#print('Total Records = {}'.format(sdfUsers.count())) 
    	return user_df

    def process_local_userfile(self,spark, filename):
	sdfUsers=readCSV().read_localcsv(spark,filename,self.user_schema)
	sdfUsers=self.read_user(spark, sdfUsers)
	self.store_userdb(sdfUsers)

    def process_s3file(self,spark,filename,bucketname):
	sdfUsers=readCSV().read_s3file(spark,filename,bucketname,self.user_schema)
	sdfUsers=self.read_s3user(spark,sdfUsers)
	sdfUsers.show(10)
	sdfUsers.printSchema()
	#sdfUsers=self.read_user(spark, sdfUsers)
	#self.store_userdb(sdfUsers)	
    def read_s3user(self, spark, df_user):
	df_user = df_user.select(
        	df_user._c0.cast("int"), 		#id
        	df_user._c1,				#login
		df_user._c2, 				#company
        	df_user._c3.cast("timestamp"), 		#created_at
		df_user._c4,				#type
		df_user._c5.cast("int"),		#fake	
		df_user._c6.cast("int"), 		#deleted
		df_user._c7.cast("decimal(10,0)"), 	#long
		df_user._c8.cast("decimal(10,0)"),	#lat
        	df_user._c9,				#country_code
		df_user._c10,				#country
		df_user._c11,				#state
		df_user._c12				#city
    	)


	df_user.withColumnRenamed('_c0', 'id').collect()
	df_user.withColumnRenamed('_c1', 'login').collect()
	df_user.withColumnRenamed('_c2', 'company').collect()
	df_user.withColumnRenamed('_c3', 'created_at').collect()
	df_user.withColumnRenamed('_c4', 'type').collect()
	df_user.withColumnRenamed('_c5', 'fake').collect()
	df_user.withColumnRenamed('_c6', 'deleted').collect()
	df_user.withColumnRenamed('_c7', 'long').collect()
	df_user.withColumnRenamed('_c8', 'lat').collect()
	df_user.withColumnRenamed('_c9', 'country_code').collect()
	df_user.withColumnRenamed('_c10', 'country').collect()
	df_user.withColumnRenamed('_c11', 'state').collect()
	df_user.withColumnRenamed('_c12', 'city').collect()
	#colname=["id","login","company","created_at","type","fake","deleted","long","lat","country_code","country","state","city"]
	#df_user.toDF(*colname)
	df_user.printSchema()
	#df_user.toDF('id', 'login','company','created_at','type','fake', 'deleted', 'long','lat','country_code', 'country','state','city')
	return df_user
		
#__name__ == '__main__':
#    spark = SparkSession \
#        .builder \
#        .appName("storing users to db") \
#        .getOrCreate()
#    user1=Users()	   
#    user1.process_local_userfile(spark, "data/users.csv")   
