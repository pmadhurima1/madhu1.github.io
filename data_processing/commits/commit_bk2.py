from __future__ import print_function
from dbconnect.PostgresConnector import PostgresConnector
from load.readCSV import readCSV 
from pyspark.sql import SparkSession,SQLContext
from pyspark.sql.types import * 
import sys

class Commit(object):
    def __init_(self):
	pass
    
    def get_commitschema(self):
	schema_commit=StructType([
            StructField("c_id", IntegerType(), True),
            StructField("sha", StringType(), True),
            StructField("author_id", IntegerType(), True),
            StructField("committer_id", IntegerType(), True),
            StructField("project_id", IntegerType(), True),
            StructField("created_at", TimestampType(), True)
        ])
	return schema_commit


 	# Main Calls this function to process local files		
    def process_local(self,spark,commit_filepath, projlang_filepath ):
	df_commit=readCSV().read_localcsv_noschema(spark,commit_filepath)
	df_commit=self.filter_commit(spark,df_commit)
	df_pl=readCSV().read_localcsv_noschema(spark,projlang_filepath)
	df_result=self.process_data(spark, df_commit,df_pl)
	df_result.show(20)
	self.store_commits(df_result)
    
 	
	
    # Main calls this function to process s3 files
    def process_s3commits(self,spark,filepath_commit, filename_projlang,bucketname):
	###print ('GHT<> project lang show')
        #df_pl=self.read_custompl(spark)
        #df_pl.show(10)
	df_commit=readCSV().read_s3file_filepath(spark,filepath_commit,self.get_commitschema() )
	df_commit=df_commit.sort("committer_id")
	df_commit=df_commit.repartition(100)
	
	df_commit.show(10)
	###print('GHT<> start commit filter')
	df_commit=self.filter_commit(spark,df_commit)
	for i in  range(1,137611262,5000):
		start= 1
		end = start + 4999
		df_pl=self.read_custompl(spark,start, end )
		df_res=self.process_data(spark, df_commit,df_pl)
        	#----------------------------------------
        	#print ('GHT <> Store results')
        	self.store_commits(df_res)
	
	#df_pl.createOrReplaceTempView("plview")
        

	#tablename="pltable"
        #query ="SELECT project_id, language from "+tablename+" limit 20"
        #df2=self.dftable_query(spark, df_pl,query, tablename)
	#print ('GHT<> end pltableview ')
        #df2.printSchema()
	#df2.show()

	#df_commit.write.parquet("hdfs://ec2-100-21-199-99.us-west-2.compute.amazonaws.com:9000/df_commit.parquet", partitionBy="project_id", mode ="append")
	#print ('GHT<> end commit show')
        #df_commit.show(10)  # first join Works time: 17 mins
        ##df_pl=self.read_custompl(spark)
	##---------------------------------------
        ##df_pl=readCSV().read_s3file(spark,filename_projlang,bucketname)
	#print ('GHT<> project lang join with commit')
        #df_res=self.process_data(spark, df_commit,df_pl)	
	#----------------------------------------
	#print ('GHT <> Store results')
	#self.store_commits(df_res)
        return 

    def filter_commit(self,spark,df_commit):
	df_user = PostgresConnector().read_df(spark.sparkContext,"(select id from users order by id )as userid")
	df_user=df_user.select(
                df_user.id#.cast("string")       # id                   
                ).repartition(10)
	#df_user.show(5)
	#df_user.printSchema()
	inner_join = df_user.join(df_commit, df_user.id == df_commit.committer_id)
	inner_join=inner_join.drop(inner_join.id)
	inner_join.printSchema() 
	###print('GHT<> joined {} users and commits', inner_join.count())
	return inner_join

	# --- common functions to local and s3 process
    def process_data(self, spark, df_commit, df_pl):	
	# format commit_df and count how many commits per language
	#df_commit=self.cast_commit(spark, df_commit)
	df_commit.orderBy(df_commit.project_id)
	###df_commit.printSchema() 

        #df_pl=self.cast_pl(spark,df_pl)
        #df_pl.show(2000)
        ###df_pl.printSchema() 
	df_result = self.count_commits(spark,df_commit,df_pl)
	###print('GHT<> process_data: after count_commits for each language')
	###df_result.show(20)
	###df_result.printSchema()
	return df_result

 
    def dftable_query(self,spark,df,query,tablename):	
	df.registerTempTable(tablename)
        df=spark.sql(query)   
        #df.show(15)                 
        #df.printSchema()    
        return df   

   	
    def count_commits(self,spark, df_commit,df_pl):
        res_commit=df_commit.join(df_pl,df_commit.project_id==df_pl.project_id)
        res_commit=res_commit.groupby(res_commit.committer_id,res_commit.language).count()
	return res_commit
    
    def read_custompl(self, spark , start, end):
	
	query = "(SELECT  * FROM pl WHERE project_id >'{}' AND project_id < '{}') as pl1 ".format(start,end)
	df_pl = PostgresConnector().read_df(spark.sparkContext,query)
	#df_pl = PostgresConnector().read_partition_pl(spark.sparkContext,"(select * from pl where project_id > $start AND project_id <$end )as pl1")
	return df_pl
    def store_commits(self, df_result):
	#df_result = df_result.withColumn("id", monotonically_increasing_id())
        #print('Total Records = {}'.format(df_result.count()))
	###df_result.show(10);
        PostgresConnector().write(df_result,'commit2','append')
        print ('commits  stored to DB')
#------------------------------------------------------------------
    def store_pll(self, df_result):
        #df_result = df_result.withColumn("id", monotonically_increasing_id())
        print('Total Records = {}'.format(df_result.count()))
        PostgresConnector().write(df_result,'pl','overwrite')
        print ('pl  stored to DB')
	  









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
   
