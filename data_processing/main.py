#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
main
'''
import logging
import sys
from userinfo.Users import Users
from followers.follower import follower
from project.projectowner import projectOwner
from commits.commit import Commit
from commits.s3list import get_filelist
from pyspark.sql.session import SparkSession

"""
Main function processes spark pipeline based on the option selected
"""

log = logging.getLogger('GHT')

def init_logger():
    '''logging '''
    # initialize logger
    #log = logging.getLogger('GHT')
    _h = logging.StreamHandler()
    _h.setFormatter(logging.Formatter("%(levelname)s  %(msg)s"))
    log.addHandler(_h)
    log.setLevel(logging.DEBUG)
    log.info("GHT logger initialized")


def main():
    ''' main call function '''
    init_logger()
    print "Starting GHTrecommend system"
    print len(sys.argv)
    if len(sys.argv) != 2:
        print "Usage: main   <option> "
        print "1: Update Users in resultdb [using local /data/users.csv]"
        print "11:Update Users in resultdb [using s3://ghtcsv/users.csv]"
        print "2: Count commits  [using local /data/commits.csv, /data/project_languages.csv]"
        print "22:Count commits  [using s3://ghtcsv/commits.csv, s3://ghtcsv/project_languages.csv]"
        print "3: Count followers  [using local /data/followers.csv]"
        print "22:Count followers  [using s3://ghtcsv/followers.csv]"
        print "9: connect to mysql sample code"
        print "Incorrect input ..exiting.."
        sys.exit(-1)

    spark = SparkSession \
        .builder \
        .appName("GHT_ETL/commit") \
        .getOrCreate()
    spark.conf.set('spark.executor.memory', '48g')
    spark.conf.set('spark.executor.cores', '18')
    spark.conf.set('spark.cores.max', '2')
    spark.conf.set('spark.driver.memory', '8g')
    choice = int(sys.argv[1])
    print "choice is "+choice

    if choice == 1:
        #user1=Users()
        log.info("calling user process- [will rewrite the user table]")
        #user1.process_local_userfile(spark, "data/users.csv")
    if choice == 11:
        #user1=Users()
        log.info("Users thr s3 data -- [will rewrite existing table ]")
        #user1.process_s3file(spark,filename="users.csv",bucketname="ghtcsv")
    if choice == 2:
        commitvar = Commit()
        log.info("Count Commits for a user [local]")
        commitvar.process_local(spark, "data/commits.csv", "data/project_languages.csv")
    if choice == 22:
        log.info("Count Commits for a user [s3 data]")
        get_filelist(spark)
        #Commit().process_s3commits(spark,"commitdata/commit.part.99902385", "pl.csv","ghtcsv");
    if choice == 3:
        log.info("Count followers for a user [local]")
        follower().process_local(spark, "data/followers.csv")
    if choice == 33:
        log.info("Count followers for a user [s3]")
        follower().process_s3(spark, "followers.csv", "ghtcsv")
    if choice == 4:
        log.info("Count projects owner by user [local]")
        projectOwner().process_local(spark, "data/projects.csv")
    if choice == 44:
        log.info("Count project owned by user [s3]")
        projectOwner().process_s3(spark, "project.csv", "ghtcsv")

if __name__ == "__main__":
    main()
