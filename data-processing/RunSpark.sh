#!/usr/bin/bash

. /home/ubuntu/.profile
/usr/local/spark/bin/spark-submit --packages com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.7,org.postgresql:postgresql:42.2.16.jre7 --master spark://10.0.0.14:7077 ~/data-processing/taxi_main.py