#!/bin/bash

echo "Extracting from $1 file $2"
 tar -xzf $1  mysql-2019-06-01/$2 -C /data/GHT/
#aws s3 cp mysql-2019-06-01/$2 s3://ghtcsv
# mv  mysql-2019-06-01/$2 /data/GHT


