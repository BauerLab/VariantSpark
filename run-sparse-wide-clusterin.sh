#!/bin/bash

input_file="/flush2/szu004/variant/phase3.sparse.bylocus.parquet"
clusters=5
iters=20

spark-submit --class au.csiro.obr17q.variantspark.SparseWideClustering \
      --master yarn-client \
      --num-executors 150 \
      --executor-memory 4G \
      --executor-cores 1 \
      --driver-memory 8G \
      variantspark/target/variantspark-0.0.1-SNAPSHOT-all.jar \
      $input_file \
      $clusters \
      $iters \
      /flush2/szu004/variant/clustering.csv
