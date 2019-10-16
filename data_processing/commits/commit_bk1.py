from __future__ import print_function
from dbconnect.PostgresConnector import PostgresConnector
from load.readCSV import readCSV 
from pyspark.sql import SparkSession,SQLContext
from pyspark.sql.types import * 
import sys

class Commit(object):
    def __init_(self):
	pass
    
    def get_schema():
	schema_commit=StructType([
            StructField("id", IntegerType(), True),
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
	print ('GHT<> project lang show')
        df_pl=self.read_custompl(spark)
        df_pl.show(10)
	df_commit=readCSV().read_s3file_filepath(spark,filepath_commit )
	df_commit.show(10)
	print('GHT<> start commit filter')
	df_commit=self.filter_commit(spark,df_commit)
	print ('GHT<> end commit show')
        df_commit.show(10)  # first join Works time: 17 mins
        ##df_pl=self.read_custompl(spark)
	##---------------------------------------
        #df_pl=readCSV().read_s3file(spark,filename_projlang,bucketname)
	print ('GHT<> project lang show')
        df_res=self.process_data(spark, df_commit,df_pl)	
	#----------------------------------------
	print ('GHT <> Store results')
	self.store_commits(df_res)
        return 

    def filter_commit(self,spark,df_commit):
	df_user = PostgresConnector().read_df(spark.sparkContext,"(select id from users order by id )as userid")
	df_user=df_user.select(
                df_user.id.cast("string")       # id                   
                )
	df_user.show(5)
	df_user.printSchema()
	df_user.registerTempTable('user')
	df_commit.orderBy('_c3').registerTempTable('commit')
	inner_join = df_user.join(df_commit, df_user.id == df_commit._c3)
	inner_join=inner_join.drop(inner_join.id)
	inner_join.show(20) 
	print('GHT<> joined {} users and commits', inner_join.count())
	return inner_join

	# --- common functions to local and s3 process
    def process_data(self, spark, df_commit, df_pl):	
	# format commit_df and count how many commits per language
	df_commit=self.cast_commit(spark, df_commit)
	df_commit.show(2000)
	df_commit.printSchema() 

        df_pl=self.cast_pl(spark,df_pl)
        df_pl.show(2000)
        df_pl.printSchema() 
	df_result = self.count_commits(df_commit,df_pl)
	print('GHT<> process_data: after count_commits for each language')
	df_result.show(20)
	df_result.printSchema()
	return df_result

 
    def dftable_query(self,spark,df,query,tablename):	
	df.registerTempTable(tablename)
        df=spark.sql(query)   
        #df.show(15)                 
        #df.printSchema()    
        return df   

    def cast_pl(self,spark, df_pl):
        df_pl=df_pl.select(
                df_pl.project_id, #.cast("int"),     	# project_id
                df_pl.language,      		#language
                )
	df_pl=df_pl.orderBy("project_id")
	#tablename="pltable"
	#query ="SELECT _c0 AS project_id, _c1 as language from "+tablename  
        #df2=self.dftable_query(spark, df_pl,query, tablename)
        #df2.printSchema()
        return df_pl
    def cast_commit(self,spark, df_commit):
	df_commit=df_commit.select(
		df_commit._c0.cast("int"), 	# id
		df_commit._c3.cast("int"),	#committer_id
		df_commit._c4.cast("int")	#project_id
		#df_commit._c5.cast("timestamp")	#created_at
		)
	tablename="committable"
        query ="SELECT _c0 AS id, _c3 AS user_id, _c4 AS project_id, _c5 AS created_at  from "+tablename
        df2=self.dftable_query(spark, df_commit,query, tablename)	
	#df2 = df_commit.selectExpr("_c0 as id","_c3 AS user_id", "_c4 as project_id", " _c5 AS created_at")	
	df2=df2.orderBy("project_id")
	#df2.printSchema()
	return df2
    	
    def count_commits(self, df_commit,df_pl):
        res_commit=df_commit.join(df_pl,df_commit.project_id==df_pl.project_id)
        res_commit=res_commit.groupby(res_commit.user_id,res_commit.language).count()
	return res_commit
    
    def read_custompl(self, spark):
	df_pl = PostgresConnector().read_df(spark.sparkContext,"(select * from pl limit 10000)as pl1")
	return df_pl
    def store_commits(self, df_result):
	#df_result = df_result.withColumn("id", monotonically_increasing_id())
        #print('Total Records = {}'.format(df_result.count()))
	df_result.show(10);
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
   
