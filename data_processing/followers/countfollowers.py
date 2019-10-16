from __future__ import print_function
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, ShortType, StringType, DecimalType, TimestampType 
import sys

# This code counts the number of projects owned in each language by the owner
def createmap(df_followers):
    df_followers.printSchema()
    df_followers=df_followers.drop(df_followers.created_at)
    df_res=df_followers.groupby(df_followers.user_id).count()
    df_res.show() #TODO Put into DATABASE

   
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

    df_followers = scSpark.read.csv(data_file, header=True,  sep=",").cache()
    createmap(df_followers);
   
