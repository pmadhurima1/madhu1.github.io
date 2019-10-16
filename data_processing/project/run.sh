


# JOINS on USERS AND PROJECTS 
#locally
python readcsv.py data/users.csv data/projects.csv
# on spark cluster



#------------------------------------------------------
hdfs dfs -copyFromLocal data.csv /user/data.csv

hdfs dfs -copyFromLocal details.csv /user/details.csv


# on cluster
spark-submit hdfs://ec2-100-20-13-37.us-west-2.compute.amazonaws.com:9000/user/data.csv hdfs://ec2-100-20-13-37.us-west-2.compute.amazonaws.com:9000/user/details.csv


# Locally
spark-submit --master spark://ec2-100-20-13-37.us-west-2.compute.amazonaws.com:7077 wordcount.py hdfs://ec2-100-20-13-37.us-west-2.compute.amazonaws.com:9000/user/alice.txt
spark-submit wordcount.py hdfs://ec2-100-20-13-37.us-west-2.compute.amazonaws.com:9000/user/alice.txt

python readcsv.py data/users.csv data/projects.csv
