#!/bin/bash


input_file="\
/flush/obr17q/phase3/ALL.chr1.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf,\
/flush/obr17q/phase3/ALL.chr2.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf,\
/flush/obr17q/phase3/ALL.chr3.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf,\
/flush/obr17q/phase3/ALL.chr4.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf,\
/flush/obr17q/phase3/ALL.chr5.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf,\
/flush/obr17q/phase3/ALL.chr6.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf,\
/flush/obr17q/phase3/ALL.chr7.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf,\
/flush/obr17q/phase3/ALL.chr8.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf,\
/flush/obr17q/phase3/ALL.chr9.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf,\
/flush/obr17q/phase3/ALL.chr10.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf,\
/flush/obr17q/phase3/ALL.chr11.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf,\
/flush/obr17q/phase3/ALL.chr12.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf,\
/flush/obr17q/phase3/ALL.chr13.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf,\
/flush/obr17q/phase3/ALL.chr14.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf,\
/flush/obr17q/phase3/ALL.chr15.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf,\
/flush/obr17q/phase3/ALL.chr16.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf,\
/flush/obr17q/phase3/ALL.chr17.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf,\
/flush/obr17q/phase3/ALL.chr18.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf,\
/flush/obr17q/phase3/ALL.chr19.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf,\
/flush/obr17q/phase3/ALL.chr20.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf,\
/flush/obr17q/phase3/ALL.chr21.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf,\
/flush/obr17q/phase3/ALL.chr22.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf\
"
clusters=5
IncludeGroups=""
ExcludeGroups=""
VariantCutoff="0"

spark-submit --class au.csiro.obr17q.variantspark.VcfClustering \
      --master yarn-client \
      --num-executors 40 \
      --executor-memory 24G \
      --executor-cores 1 \
      --driver-memory 24G \
      variantspark/target/variantspark-0.0.1-SNAPSHOT.jar \
      $input_file \
      $clusters \
      "$IncludeGroups" \
      "$ExcludeGroups" \
      $VariantCutoff
