#!/bin/sh

HADOOP_HOME=/usr/java/hadoop
INPUT=$1
OUTPUT=$2
hadoop jar $HADOOP_HOME/contrib/streaming/hadoop-streaming*.jar \
    -file mapper.py -mapper mapper.py \
    -file reducer.py -reducer reducer.py \
    -input $INPUT -output $OUTPUT
