#VCF-clusterer Readme



##Input
This should point to a directory on HDFS. This directory should contain VCF files for clustering. Alternatively you can specify a single VCF file.

##Output
VCF-Clusterer will create the output directory which will store files and directories created from pre-processing and clustering. The structure will be as follows:

###Directories:
sequence      Output of first pass of pre-processing.
transposed    Output of second pass of pre-processing. This is the input directory for Mahout k-means clustering.
clusters      k-means centers. Remove this directory if you wish to cluster with a different k value.
output        Output from clustring

###Files:
feature-size           Total number of variants. Utility file for the second pass of pre-processing.
resultFileCluster.txt  Lists the clusters and individual IDs in each cluster.
populations.txt        Lists the clusters and individual populations in each cluster.

##Parameters
Run VCF-Clusterer with the following parameters:
```
    -p            pre-process files in input directory
    -c            run k-means clustering job on pre-processed files
    -i {input}    directory containing VCF files, relative path on HDFS
    -o {output}   output directory, relative path on HDFS
    -l {0}        when pre-processing, ignore variants occuring in less than this many people
    -m {1000}     when pre-processing, ignore variants occuring in more than this many people
    -k {5}        number of clusters for k-means clustering" >&2
```
asfaf
