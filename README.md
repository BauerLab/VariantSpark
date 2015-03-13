# VCF-clusterer Readme

## Install

1. Download `vcf-cluster.sh` and `VCF-clusterer-*-SNAPSHOT.jar`
2. If Mahout is not installed on your cluster, download it to your home directory
3. Edit `vcf-cluster.sh` to point to the Mahout libraries
4. Ensure `vcf-cluster.sh` is executable `chmod +x vcf-cluster.sh`

## Building From Source

`VCF-clusterer-*-SNAPSHOT.jar` has been built for the latest Mahout distribution on Cloudera.
If you have trouble running it, you can build it yourself using Maven, either on your local machine or directly on your cluster.

1. Check out the repo
2. `cd vcf-clusterer`
3. `vi pom.xml` and edit `<dependencies>` children to refer to the Mahout and Hadoop version present on your cluster. 
4. `mvn jar:jar` to build.
5. If you built it on your local machine, copy the file, `target/VCF-clusterer-0.0.1-SNAPSHOT.jar` to your cluster.

##Submit a Job
Once installed, use the launcher script, `vcf-cluster.sh` to submit a job to your cluster.
For example, `./vcf-clusterer.sh -pc -i input_vcf_files -o output_dir -l 0 -m 1000 -k 7`.

##Parameters
Run VCF-Clusterer with the following parameters:

```
-p          # pre-process files in input directory
-c          # run k-means clustering job on pre-processed files
-i {input}  # directory containing VCF files, relative path on HDFS
-o {output} # output directory, relative path on HDFS
-l {0}      # when pre-processing, ignore variants occuring in less than this many people
-m {1000}   # when pre-processing, ignore variants occuring in more than this many people
-k {5}      # number of clusters for k-means clustering"
```

##Input
This should point to a directory on HDFS. This directory should contain VCF files for clustering. Alternatively you can specify a single VCF file.

##Output
VCF-Clusterer will create the output directory which will store files and directories created from pre-processing and clustering. The structure will be as follows:

####directories:

```
sequence      Output of first pass of pre-processing.
transposed    Output of second pass of pre-processing. This is the input directory for Mahout k-means clustering.
clusters      k-means centers. Remove this directory if you wish to cluster with a different k value.
output        Output from clustering
```

####files:

```
feature-size           Total number of variants. Utility file for the second pass of pre-processing.
resultFileCluster.txt  Lists the clusters and individual IDs in each cluster.
populations.txt        Lists the clusters and individual populations in each cluster.
```

