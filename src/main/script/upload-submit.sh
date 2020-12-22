#!/bin/bash
#export HADOOP_USER_NAME=szabi
export ALAKKA_ROOT="/Users/szabolcsbeki/Documents/GitHub/alakka"
export HDFS_ALAKKA_ROOT="hdfs://szabi-1.lab.eng.hortonworks.com:8020/user/szabi/alakka"

hdfs dfs -put -f $ALAKKA_ROOT/target/scala-2.11/alakka_2.11-0.1-SNAPSHOT.jar $HDFS_ALAKKA_ROOT
#hdfs dfs -put -f $ALAKKA_ROOT/target/scala-2.11/alakka-assembly-1.0.jar $HDFS_ALAKKA_ROOT
curl -X POST -H "Content-Type: application/json" -H "X-Requested-By: szabi" \
 --data @$ALAKKA_ROOT/src/main/script/livy-batch.json http://szabi-4:8999/batches
