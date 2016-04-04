# Input
input_file="data/coadread_mutations_bmi_filtered.csv"

# Parameters
trees=250
depth=5
bins=20
feature_subset="auto" # auto, all, sqrt, log2, onethird
classification="false"
label="bmi"
VariantCutoff="0"
folds="2"

spark-submit \
      --class au.csiro.obr17q.variantspark.VcfForest \
      --master yarn-client \
      --num-executors 64 \
      --executor-memory 2G \
      --executor-cores 1 \
      --driver-memory 4G \
      target/variantspark-0.0.1-SNAPSHOT.jar \
      $input_file \
      $trees \
      $depth \
      $bins \
      $feature_subset \
      $classification \
      $label \
      $VariantCutoff \
      $folds
