from __future__ import print_function
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, ShortType, StringType, DecimalType, TimestampType 
import sys

def createmap(df_issues):
    df_issues.printSchema()
    df_issues1=df_issues.select(df_issues.id, df_issues.reporter_id)
    df_issues2=df_issues.select(df_issues.id, df_issues.assignee_id) 	
    df_res=df_issues1.groupby(df_issues.reporter_id).count()
    df_res2=df_issues2.groupby(df_issues.assignee_id).count()	
    
    df_res.show() #TODO Put into DATABASE
    df_res2.show()		
    #df_commits=df_commits.drop(df_commits.author_id) 
    #df_proj=df_proj.select(df_proj.id, df_proj.language)    
    #df_plang=df_plang.drop(df_plang.created_at)

   
if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: countissues <issues.csv> ", file=sys.stderr)
        sys.exit(-1)	
    scSpark = SparkSession \
        .builder \
        .appName("counting issues") \
        .getOrCreate()
    data_file = sys.argv[1] #'/Development/PetProjects/LearningSpark/data.csv'
    #detail_file=sys.argv[2]

    df_issues = scSpark.read.csv(data_file, header=True,  sep=",").cache()
    createmap(df_issues);
   
