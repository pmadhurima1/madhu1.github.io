from __future__ import print_function
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, ShortType, StringType, DecimalType, TimestampType 
import sys

# This code counts the number of projects owned in each language by the owner
def store_userdb(sdfUsers):
    print('Total Records = {}'.format(sdfUsers.count()))
    sdfUsers.show() # TODO Send to database
   
def count_projectowners(sdfUsers, sdfProjects):
    print('Total Records = {}'.format(sdfUsers.count()))
    sdfUsers.show()
    print('Total details = {}'.format(sdfProjects.count()))
    sdfProjects= sdfProjects.select(sdfProjects.owner_id, sdfProjects.language,sdfProjects.updated_at)
    sdfProjects.show()
    newUsers=sdfUsers.select(sdfUsers.id)
    res_lang = sdfProjects.join(newUsers, newUsers.id==  sdfProjects.owner_id).drop(sdfProjects.owner_id)
    res_lang.printSchema()
    res_lang=res_lang.groupBy(res_lang.id, res_lang.language).count()
    print('Result data frame: ')
    res_lang.show() # TODO Send to database 
    newUsers=sdfUsers.select(sdfUsers.id)
    res_lang = sdfProjects.join(newUsers, newUsers.id==  sdfProjects.owner_id).drop(sdfProjects.owner_id)
    res_lang.printSchema()
    res_lang=res_lang.groupBy(res_lang.id, res_lang.language).count()
    print('Result data frame: ')
    res_lang.show()


   
if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: countowners  <file1> <file2>", file=sys.stderr)
        sys.exit(-1)	
    scSpark = SparkSession \
        .builder \
        .appName("counting the owners") \
        .getOrCreate()
    data_file = sys.argv[1] #'/Development/PetProjects/LearningSpark/data.csv'
    detail_file=sys.argv[2]

    sdfUsers = scSpark.read.csv(data_file, header=True,  sep=",").cache()
    sdfProjects = scSpark.read.csv(detail_file, header=True, sep=",").cache()
    sdfUsers=sdfUsers.select(sdfUsers.id,sdfUsers.login, sdfUsers.name, sdfUsers.country_code, sdfUsers.state,sdfUsers.city)
    store_userdb(sdfUsers)
    count_projectowners(sdfUsers, sdfProjects)
    
    #Creating results data frame 
    #Part 1 :  # project owned +  last updated date 

    #sdfUsers.write.csv('outputuser', header='true')
    
    	
    # Code to count number of project languages appear in sdfProjects 
    #sdflang_cnt=sdfProjects.groupBy(sdfProjects.language).count()
    #sdflang_cnt.show()
   
