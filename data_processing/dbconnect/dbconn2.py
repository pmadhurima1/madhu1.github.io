import psycopg2

try:
    conn = psycopg2.connect("dbname='resultdb' user='dbuser' host='ec2-54-189-185-32.us-west-2.compute.amazonaws.com' password='passdb'")

except psycopg2.OperationalError as ex:
    print("Connection failed: {0}".format(ex.cursor()));
except:print "I am unable to connect to the database"
cur = conn.cursor()
cur.execute("""SELECT * from lang_results """)
rows = cur.fetchall()

print "\nShow me the databases:\n"
for row in rows:
    print "   ", row[0] 
    print "   ", row[1]
    print "   ", row[2]
