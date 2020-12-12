#!/bin/bash
#export HADOOP_USER_NAME=szabi
export ALAKKA_ROOT="/Users/szabolcsbeki/Documents/GitHub/alakka"
export HDFS_ALAKKA_ROOT="hdfs://szabi-1.lab.eng.hortonworks.com:8020/user/szabi/alakka"


curl -X POST -H "Content-Type: application/json" -H "X-Requested-By: szabi"  \
  --data @$ALAKKA_ROOT/src/main/script/livy-batch.json http://szabi-4:8999/batches
