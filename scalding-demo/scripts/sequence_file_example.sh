#!/usr/bin/env bash

hadoop jar target/scalding-demo-1.0-SNAPSHOT.jar \
    com.twitter.scalding.Tool \
    -Dmapreduce.framework.name=local -Dfs.defaultFS=file:/// \
    SequenceFileExample \
    --hdfs \
    --input testdata/seq \
    --output output
