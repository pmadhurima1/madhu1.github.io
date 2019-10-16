from __future__ import print_function
from dbconnect.PostgresConnector import PostgresConnector
from load.readCSV import readCSV 
from pyspark.sql import SparkSession,SQLContext
from pyspark.sql.types import * 
import sys

class projectOwner (object):
    def __init_(self):
	pass
	
 	# Main Calls this function to process local files		
    def process_local(self,spark,project_filepath ):
	df_proj=readCSV().read_localcsv_noschema(spark,project_filepath)
	df_user=self.get_User(spark)
	df_proj=self.cast_proj(spark, df_proj)
	df_proj.show(20)
	df_user.show(20)
	df_result=self.count_ownedprojects(df_user,df_proj)
	#df_result.show(20)
	self.store_ownercount(df_result)

	
    # Main calls this function to process s3 files
    def process_s3(self,spark,filename_proj, bucketname):
        df_proj=readCSV().read_s3file(spark,filename_proj,bucketname)
	df_user=self.get_User(spark)
	df_proj=self.cast_proj(spark, df_proj)
	df_proj.show(20)
	df_proj.printSchema()
	df_result=self.count_ownedprojects( df_user,df_proj)
	self.store_ownercount(df_result)
        

    def get_User(self,spark):
	df_user = PostgresConnector().read_df(spark.sparkContext,'(select id from users)as userid')
	return df_user

	# --- common functions to local and s3 process

 
    def dftable_query(self,spark,df,query,tablename):	
	df.registerTempTable(tablename)
        df=spark.sql(query)   
        df.show(15)                 
        df.printSchema()    
        return df   

    def cast_proj(self,spark, df_proj):
	df_proj=df_proj.select(
		df_proj._c0.cast("int"), 	# id
		df_proj._c2.cast("int"),	#owner_id
		df_proj._c5			#language
		)
	tablename="projtable"
        query =" SELECT _c0 AS id, _c2 AS owner_id,_c5 AS language from  "+tablename
        df2=self.dftable_query(spark, df_proj,query, tablename)
	return df2
    	
    def count_ownedprojects(self, df_user,df_proj):
	df_proj=df_proj.drop(df_proj.id)
    	res_lang = df_proj.join(df_user, df_user.id==df_proj.owner_id).drop(df_proj.owner_id)
    	res_lang.printSchema()
    	res_lang=res_lang.groupBy(res_lang.id, res_lang.language).count()
    	print('Result data frame: ')
    	res_lang.show(10) # TODO Send to database
	return res_lang

    def store_ownercount(self, df_result):
	#df_result = df_result.withColumn("id", monotonically_increasing_id())
        #print('Total Records = {}'.format(df_result.count()))
	df_result.show(10);
        PostgresConnector().write(df_result,'profile','overwrite')
        print ('owner count  stored to DB')


  









if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: spark-submit countcommits.py  <commits.csv> <projects_language>", file=sys.stderr)
        sys.exit(-1)	
    scSpark = SparkSession \
        .builder \
        .appName("counting commits") \
        .OrCreate()
    commit_file = sys.argv[1] #'/Development/PetProjects/LearningSpark/data.csv'
    projlang_file=sys.argv[2]
    print ('proj_lang file path{} ', projlang_file)	
    commit_var= Commit()
    commit_val.process_local(spark,commit_file)			
    #df_commits = scSpark.read.csv(commit_file, header=True,  sep=",").cache()
    #df_commits.show()	
    #df_proj = scSpark.read.csv(projlang_file, header=True, sep=",").cache()
    #df_proj.show()	
    #createmap(df_commits, df_proj);
   
