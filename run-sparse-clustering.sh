#!/bin/bash

input_file="/flush2/szu004/variant/phase3.sparse.parquet"
clusters=5
IncludeGroups=""
ExcludeGroups=""
VariantCutoff="0"

spark-submit --class au.csiro.obr17q.variantspark.SparseClustering \
      --master yarn-client \
      --num-executors 50 \
      --executor-memory 16G \
      --executor-cores 1 \
      --driver-memory 24G \
      variantspark/target/variantspark-0.0.1-SNAPSHOT.jar \
      $input_file \
      $clusters \
      "$IncludeGroups" \
      "$ExcludeGroups" \
      $VariantCutoff
