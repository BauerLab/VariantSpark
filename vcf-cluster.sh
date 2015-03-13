##
## Specify installation-specific Mahout parameters on the following lines.
##

#Ddirectory containing Mahout libraries.
MAHOUT_LIBS=~/mahout-0.9-cdh5.3.0/

#Mahout libraries to use, confirm the file names and versions are correct.
#These files are located in $MAHOUT_LIBS.
export LIBJARS=\
${MAHOUT_LIBS}mahout-core-0.9-cdh5.3.0.jar,\
${MAHOUT_LIBS}mahout-core-0.9-cdh5.3.0-job.jar,\
${MAHOUT_LIBS}mahout-math-0.9-cdh5.3.0.jar,\
${MAHOUT_LIBS}mahout-examples-0.9-cdh5.3.0.jar,\
${MAHOUT_LIBS}mahout-examples-0.9-cdh5.3.0-job.jar,\
${MAHOUT_LIBS}mahout-integration-0.9-cdh5.3.0.jar,\
${MAHOUT_LIBS}lib/commons-cli-2.0-mahout.jar,\
${MAHOUT_LIBS}lib/commons-collections-3.2.1.jar,\
${MAHOUT_LIBS}lib/commons-math3-3.2.jar,\
${MAHOUT_LIBS}lib/lucene-core-4.6.1.jar

##
## You shouldn't need to edit anything below this line
##

export HADOOP_CLASSPATH=`echo ${LIBJARS} | sed s/,/:/g`

# Default values
cutoff_min=0
cutoff_max=9999
input="/flush/obr17q/phase1_1-3"
jobid="phase1_chr1-3_cutoff${cutoff_min}-${cutoff_max}"
iterations=20
k=5
tasks=()


# Parse options
while getopts ":hl:m:pck:i:o:" opt; do
  case $opt in
    p)
      tasks[0]='pre-processing'
      ;;
    c)
      tasks[1]='clustering'
      ;;
    k)
      k=$OPTARG >&2
      ;;
    l)
      cutoff_min=$OPTARG >&2
      ;;
    m)
      cutoff_max=$OPTARG >&2
      ;;
    i)
      input=$OPTARG >&2
      ;;
    o)
      output=$OPTARG >&2
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

echo "Performing tasks: ${tasks[@]}"
echo "Command line arguments: {-i=[$input], -o=[$jobid], -k=[$k], -l[$cutoff_min], -m[$cutoff_max]}"

if [ ! -z ${tasks[0]} ]
  then
    echo "Pre-processing data"
    yarn jar VCF-clusterer-0.0.1-SNAPSHOT.jar au.csiro.StageOne -libjars ${LIBJARS} "${input}" "${jobid}" "${cutoff_min}" "${cutoff_max}"
    yarn jar VCF-clusterer-0.0.1-SNAPSHOT.jar au.csiro.StageTwo -libjars ${LIBJARS} "${jobid}"
fi

if [ ! -z ${tasks[1]} ]
  then
    echo "Clustering data"
    yarn jar VCF-clusterer-0.0.1-SNAPSHOT.jar au.csiro.KmeansClusterer -libjars ${LIBJARS} "${jobid}" "${k}" "3.1" "1.2" "${iterations}"
fi

if [ ! -z ${tasks[2]} ]
  then
    echo "Adjusted Rand Indexing data"
    yarn jar VCF-clusterer-0.0.1-SNAPSHOT.jar au.csiro.GetResults -libjars ${LIBJARS} "${jobid}" "0" "phase3.txt"
fi

