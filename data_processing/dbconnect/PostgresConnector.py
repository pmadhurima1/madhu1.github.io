'''
Class for Database connection
'''

from pyspark.sql import DataFrameWriter, DataFrameReader
#import configparser
from pyspark.sql import SQLContext

class PostgresConnector():
    '''
    Connect to db to perform read/write operations to database
    '''
    db_url = ''
    properties = {}
    def __init__(self):
        self.db_url = 'jdbc:postgresql://ip-10-0-0-14:5432/resultdb'
        self.properties = {'user':'dbuser', 'password':'passwd', 'driver':'org.postgresql.Driver'}


    def read_df(self, spark_context, tablename):
        ''' read from jdbc'''
        print self.properties
        sql_context = SQLContext(spark_context)
        data_frame = DataFrameReader(sql_context).jdbc(url=self.db_url, table=tablename, \
        properties=self.properties)
        return data_frame

    def read_partition_pl(self, spark_context, tablename):
        '''read from jdbc with partition '''
        sql_context = SQLContext(spark_context)
        data_frame = DataFrameReader(sql_context).jdbc(
            url=self.db_url, table=tablename, lowerBound=0, upperBound=56326087, \
            numPartitions=100, \
            properties=self.properties)
        return data_frame

    def get_writer(self, data_frame):
        ''' gets writer '''
        return DataFrameWriter(data_frame)

    def write(self, data_frame, table, mode):
        ''' write to jdbc'''
        my_writer = self.get_writer(data_frame)
        my_writer.jdbc(url=self.db_url, table=table, mode=mode, properties=self.properties)
