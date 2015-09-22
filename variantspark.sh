# Default configuration
StartPosition=0
EndPosition=2147483647
IncludeGroups=""
ExcludeGroups=""
SampleSize=1

# Parse options
while getopts ":hc:" opt; do
  case $opt in
    c)
      config_file=$OPTARG >&2
      if [ ! -f $config_file ]; then
        echo "Config file not found"
        exit 1
      fi
      source $config_file
      ;;
    h)
      echo "Usage: vcf-clusterer [-pc] [-i input] [-o output] [-k clusters]
  -p    pre-process files in input directory
  -c    run k-means clustering job on pre-processed files
  -i    directory containing VCF files, relative path on HDFS
  -o    output directory, relative path on HDFS
  -l    when pre-processing, ignore variants occuring in less than this many people
  -m    when pre-processing, ignore variants occuring in more than this many people
  -k    number of clusters for k-means clustering" >&2
      exit 1
      ;;
    \?)
      echo "Invalid option: -$OPTARG
Try vcf-clusterer -h for more information." >&2
      exit 1
      ;;
    :)
      echo "Option -$OPTARG requires an argument.
Try vcf-clusterer -h for more information." >&2
      exit 1
      ;;
  esac
done

spark-submit --class au.csiro.obr17q.variantspark.VcfClustering \
      --master yarn-client \
      --num-executors $num_executors \
      --executor-memory $executor_memory \
      --executor-cores $executor_cores \
      --driver-memory $driver_memory \
      workspace/variantspark/target/variantspark-0.0.1-SNAPSHOT.jar \
      $input_file \
      $StartPosition \
      $EndPosition \
      $clusters \
      "$IncludeGroups" \
      "$ExcludeGroups" \
      $SampleSize
