#!/bin/bash


spark-submit --class au.csiro.obr17q.variantspark.VcfSparseForest \
      --master yarn-client \
      --num-executors 50 \
      --executor-memory 12G \
      --executor-cores 1 \
      --driver-memory 12G \
      variantspark/target/variantspark-0.0.1-SNAPSHOT-all.jar \
      /flush2/szu004/variant/phase3.sparse.parquet \
      10 \
      auto \
      10 \
      10
