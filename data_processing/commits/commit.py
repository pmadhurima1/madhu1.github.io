'''
Process commits for user according to language
'''
from __future__ import print_function
import sys
from dbconnect.PostgresConnector import PostgresConnector
from load.readCSV import readCSV
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, TimestampType, StructField, StructType, StringType

class Commit(object):
    '''Class for processing the commit '''
    def __init_(self):
        pass
    @staticmethod
    def get_commitschema():
        ''' table schema  '''
        schema_commit = StructType([
            StructField("c_id", IntegerType(), True),
            StructField("sha", StringType(), True),
            StructField("author_id", IntegerType(), True),
            StructField("committer_id", IntegerType(), True),
            StructField("project_id", IntegerType(), True),
            StructField("created_at", TimestampType(), True)
        ])
        return schema_commit

    def process_local(self, spark, commit_filepath, projlang_filepath):
        '''  function to process local files '''
        df_commit = readCSV().read_localcsv_noschema(spark, \
           commit_filepath)
        df_commit = self.filter_commit(spark, df_commit)
        df_pl = readCSV().read_localcsv_noschema(spark, projlang_filepath)
        df_result = self.process_data(df_commit, df_pl)
        df_result.show(20)
        self.store_commits(df_result)

    def process_s3commits(self, spark, filepath_commit, filename_projlang, bucketname):
        ''' process S3 commit data'''
        df_commit = readCSV().read_s3file_filepath(
            spark, filepath_commit, self.get_commitschema())
        df_commit = df_commit.sort("committer_id")
        df_commit = df_commit.repartition(100)
        df_commit.show(10)
        ###print('GHT<> start commit filter')
        df_commit = self.filter_commit(spark, df_commit)
        size_commit = 137611262
        chunk_size = 10000
        for i in range(1, size_commit, chunk_size):
            start = i
            end = start + chunk_size
            df_pl = self.read_custompl(spark, start, end)
            df_res = self.process_data(df_commit, df_pl)
            # ----------------------------------------
            #print ('GHT <> Store results')
            self.store_commits(df_res)
        return

    @staticmethod
    def filter_commit(spark, df_commit):
        ''' reduce the commit based on users'''
        df_user = PostgresConnector().read_df(spark.sparkContext, "(select id from users \
                order by id )as userid")
        df_user = df_user.select(
            df_user.id
        ).repartition(10)
        inner_join = df_user.join(
            df_commit, df_user.id == df_commit.committer_id)
        inner_join = inner_join.drop(inner_join.id)
        inner_join.printSchema()
        return inner_join


    def process_data(self, df_commit, df_pl):
        ''' generate for each project language'''
        df_commit.orderBy(df_commit.project_id)
        df_result = self.count_commits(df_commit, df_pl)
        return df_result

    @staticmethod
    def dftable_query(spark, data_frame, query, tablename):
        ''' query on table'''
        data_frame.registerTempTable(tablename)
        data_frame = spark.sql(query)
        return data_frame

    @staticmethod
    def count_commits(df_commit, df_pl):
        '''Counts commits using joins on project language table '''
        res_commit = df_commit.join(
            df_pl, df_commit.project_id == df_pl.project_id)
        res_commit = res_commit.groupby(
            res_commit.committer_id, res_commit.language).count()
        return res_commit

    @staticmethod
    def read_custompl(spark, start, end):
        '''function : query pl from database'''
        query = "(SELECT  * FROM pl WHERE project_id >'{}' \
           AND project_id < '{}') as pl1 ".format(start, end)
        df_pl = PostgresConnector().read_df(spark.sparkContext, query)
        return df_pl

    @staticmethod
    def store_commits(df_result):
        '''function to store commits to database '''
        #print('Total Records = {}'.format(df_result.count()))
        PostgresConnector().write(df_result, 'commit2', 'append')
        print ('commits  stored to DB')

    @staticmethod
    def store_pll(df_result):
        ''' store project language to database'''
        print('Total Records = {}'.format(df_result.count()))
        PostgresConnector().write(df_result, 'pl', 'overwrite')
        print ('pl  stored to DB')


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: spark-submit countcommits.py  <commits.csv> \
              <projects_language>", file=sys.stderr)
        sys.exit(-1)
    SPARK_S = SparkSession \
        .builder \
        .appName("counting commits") \
        .OrCreate()
    COMMIT_FILE = sys.argv[1]
    LANGUAGE_FILE = sys.argv[2]
    COMMIT_VAR = Commit()
    COMMIT_VAR.process_local(SPARK_S, COMMIT_FILE, LANGUAGE_FILE)
