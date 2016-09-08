## Define the following 2 parameters
input_file="ALL.chr22.SHAPEIT2_integrated_phase1_v3.20101123.snps_indels_svs.genotypes.all.vcf"
clusters=4


spark-submit --class au.csiro.obr17q.variantspark.VcfClustering \
      --master yarn-client \
      --num-executors 32 \
      --executor-memory 2G \
      --executor-cores 1 \
      --driver-memory 4G \
      target/variantspark-0.0.1-SNAPSHOT.jar \
      $input_file \
      $clusters
