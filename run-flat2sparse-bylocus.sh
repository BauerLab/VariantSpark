#!/bin/bash

rm -rf /flush2/szu004/variant/phase3.sparse.bylocus.parquet

spark-submit --class au.csiro.obr17q.variantspark.FlatVariantToSparseByPosition \
      --master yarn-client \
      --num-executors 100 \
      --executor-memory 8G \
      --executor-cores 1 \
      --driver-memory 8G \
      variantspark/target/variantspark-0.0.1-SNAPSHOT-all.jar \
      /flush2/szu004/variant/phase3.flat.parquet \
      /flush2/szu004/variant/phase3.sparse.bylocus.parquet \
      1000
