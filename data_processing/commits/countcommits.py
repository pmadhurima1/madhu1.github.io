from __future__ import print_function
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, ShortType, StringType, DecimalType, TimestampType 
import sys

# This code counts the number of projects owned in each language by the owner
def createmap(df_commits,df_proj):
    df_commits.printSchema()
    df_proj.printSchema()	
    df_commits=df_commits.drop(df_commits.sha)
    df_commits=df_commits.drop(df_commits.author_id) 
    df_proj=df_proj.select(df_proj.id, df_proj.language)    
    #df_plang=df_plang.drop(df_plang.created_at)
    res_commits=df_commits.join(df_proj,df_commits.project_id==df_proj.id).drop(df_proj.id)
    res_commits=res_commits.groupby(res_commits.committer_id,res_commits.language).count()	 
    res_commits.show() 	# TODO Update this into commits column  

   
if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: spark-submit countcommits.py  <commits.csv> <projects_language>", file=sys.stderr)
        sys.exit(-1)	
    scSpark = SparkSession \
        .builder \
        .appName("counting commits") \
        .getOrCreate()
    commit_file = sys.argv[1] #'/Development/PetProjects/LearningSpark/data.csv'
    projlang_file=sys.argv[2]
    print ('proj_lang file path{} ', projlang_file)	
    df_commits = scSpark.read.csv(commit_file, header=True,  sep=",").cache()
    df_commits.show()	
    df_proj = scSpark.read.csv(projlang_file, header=True, sep=",").cache()
    df_proj.show()	
    createmap(df_commits, df_proj);
   
