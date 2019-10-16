 
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext('local')
spark = SparkSession(sc)

# connection url and query
url = 'jdbc:postgresql://ip-10-0-0-14.us-west-2.compute.internal:5432/resultdb?user=dbuser&password=passdb'
dbtable = '(select * from lang_results) as lang'

# load the tables into dataframe
pgsql_df = spark.read.format('jdbc').options(url=url,dbtable=dbtable).load()
 
# printschema and inspect sample rows
pgsql_df.printSchema()
pgsql_df.show()
