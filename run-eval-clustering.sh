#!/bin/bash

spark-submit --class au.csiro.obr17q.variantspark.EvalClustering \
      --master local \
      variantspark/target/variantspark-0.0.1-SNAPSHOT-all.jar \
      /flush2/szu004/variant/clustering.csv
