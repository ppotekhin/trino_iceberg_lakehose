#!/bin/bash
set -e

$SPARK_HOME/sbin/start-connect-server.sh  \
  --packages org.apache.spark:spark-connect_2.12:3.5.5,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262

tail -f /dev/null