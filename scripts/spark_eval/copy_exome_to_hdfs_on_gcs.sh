#!/usr/bin/env bash

# Copy exome data to HDFS on a GCS cluster.

${GATK_HOME:-../..}/gatk-launch ParallelCopyGCSDirectoryIntoHDFSSpark \
    --inputGCSPath gs://broad-spark-eval-test-data/exome/ \
    --outputHDFSDirectory hdfs://${GCS_CLUSTER}-m:8020/user/$USER/exome_spark_eval \
    -apiKey $API_KEY \
    -- \
    --sparkRunner GCS \
    --cluster $GCS_CLUSTER