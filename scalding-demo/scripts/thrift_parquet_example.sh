#!/usr/bin/env bash

~/tools/infra-client/bin/hadoop --cluster lgprc-xiaomi2 jar target/scalding-demo-1.0-SNAPSHOT.jar \
    com.twitter.scalding.Tool \
    -Dmapreduce.framework.name=local -Dfs.defaultFS=file:/// \
    ThriftParquetFileExample \
    --hdfs \
    --input testdata/name.parquet \
    --output output
