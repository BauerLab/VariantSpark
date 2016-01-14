#!/bin/bash

spark-submit --class au.csiro.obr17q.variantspark.ValidateSparse \
      --master yarn-client \
      --num-executors 200 \
      --executor-memory 4G \
      --executor-cores 1 \
      --driver-memory 4G \
      variantspark/target/variantspark-0.0.1-SNAPSHOT-all.jar \
      /flush2/szu004/variant/phase3.sparse.parquet 
