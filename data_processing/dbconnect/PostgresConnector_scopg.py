from pyspark.sql import DataFrameWriter
import psycopg2

class PostgresConnector():
    def __init__():
        self.database_name = 'resultdb'
        self.hostname = 'ip-10-0-0-14'
        self.username = 'dbuser'
	self.password='passdb'

    def getConnection():
	try:
            conn = pyscopg2.connect("dbname=self.database_name user=self.username host=self.hostname password=self.password " ) ;
        except psycopg2.OperationalError as ex:
    	    print("Connection failed: {0}".format(ex.cursor()));
	except:print "Unable to connect to the database"
	return conn;
    
    def get_writer(self, df):
        return DataFrameWriter(df)

    def write(self, df, table, mode):
        my_writer = self.get_writer(df)
        my_writer.jdbc(self.url_connect, table, mode, self.properties)
