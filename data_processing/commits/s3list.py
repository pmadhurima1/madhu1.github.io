import boto3
from commit import Commit
def get_filelist(spark):
	prefix="commitdata/"
	s3 = boto3.resource('s3')
	bucket = s3.Bucket(name="ghtcsv")
	FilesNotFound = True
	#for obj in bucket.objects.filter(Prefix=prefix):
	     #print('{0}:{1}'.format(bucket.name, obj.key))
     	     #FilesNotFound = False
	if FilesNotFound:
    	     print("ALERT", "No file in {0}/{1}".format(bucket, prefix))
	Commit().process_s3commits(spark,"s3a://ghtcsv/commitdata/commit.part.00", "pl.csv","ghtcsv");
