#!/bin/bash
#PBS -N hadoop_job
#PBS -l nodes=2:ppn=12,vmem=48GB
#PBS -l walltime=60:00
#PBS -j oe

### load modules to set the appropriate variables
# especially HADOOP_DATA_DIR and HADOOP_LOG_DIR
module load jdk/1.7.0_11 hadoop/2.2.0 hpchadoop openmpi
# avoid some warnings... HADOOP_ROOT is used by ASC hpcHadoop instead
test -n "$HADOOP_HOME" && unset HADOOP_HOME



###
# Classpaths for jars that are distributed to nodes
export LIBJARS=/home/cci/obr17q/mahout-distribution-0.9/mahout-core-0.9.jar,/home/cci/obr17q/mahout-distribution-0.9/mahout-math-0.9.jar,/home/cci/obr17q/mahout-distribution-0.9/mahout-integration-0.9.jar,/home/cci/obr17q/mahout-distribution-0.9/lib/commons-cli-2.0-mahout.jar,/home/cci/obr17q/mahout-distribution-0.9/lib/commons-collections-3.2.1.jar,/home/cci/obr17q/mahout-distribution-0.9/lib/guava-16.0.jar,/home/cci/obr17q/mahout-distribution-0.9/lib/commons-math3-3.2.jar
export HADOOP_CLASSPATH=/home/cci/obr17q/mahout-distribution-0.9/mahout-core-0.9.jar:/home/cci/obr17q/mahout-distribution-0.9/mahout-math-0.9.jar:/home/cci/obr17q/mahout-distribution-0.9/mahout-integration-0.9.jar:/home/cci/obr17q/mahout-distribution-0.9/lib/commons-cli-2.0-mahout.jar:/home/cci/obr17q/mahout-distribution-0.9/lib/commons-collections-3.2.1.jar:/home/cci/obr17q/mahout-distribution-0.9/lib/guava-16.0.jar:/home/cci/obr17q/mahout-distribution-0.9/lib/commons-math3-3.2.jar



### These may not actually be required..
# I added them during some trial and error
export HADOOP_HOME=/apps/hadoop/2.2.0
export HADOOP_MAPRED_HOME=/apps/hadoop/2.2.0
export HADOOP_COMMON_HOME=/apps/hadoop/2.2.0
export HADOOP_HDFS_HOME=/apps/hadoop/2.2.0
export HADOOP_YARN_HOME=/apps/hadoop/2.2.0






#### Set this to the directory where Hadoop configs should be generated
# this will be populated just for this Hadoop instance
#
# Make sure that this is accessible to all nodes
export HADOOP_CONF_DIR=$HOME/.hpchadoop/$PBS_JOBID
export YARN_CONF_DIR=$HADOOP_CONF_DIR

#export HADOOP_LOG_DIR=$HOME/mylogs

#### Set up the configuration using hpcHadoop
# usage: hpchadoop -h
echo "Setting up the configurations for hpcHadoop"
# this is the non-persistent mode
hpchadoop -c $HADOOP_CONF_DIR


### Copy the default configuration files. These are probably in /app/hado... actually, 
# so could probably copy from there if so.
cp $HOME/conf/yarn-default.xml $HADOOP_CONF_DIR
cp $HOME/conf/mapred-default.xml $HADOOP_CONF_DIR


### Creates the yarn-site.xml file
# was originally just going to be a few lines..
# First few options are essential
# Latter ones are memory options for a whole node. 

echo -e "<?xml version=\"1.0\"?>
<?xml-stylesheet type=\"text/xsl\" href=\"configuration.xsl\"?>
<configuration>
<property>
  <name>yarn.resourcemanager.hostname</name>
  <value>$HOSTNAME</value>
</property>
<property>
  <name>yarn.nodemanager.hostname</name>
  <value>0.0.0.0</value>
</property>
<property>
  <name>yarn.nodemanager.aux-services</name>
  <value>mapreduce_shuffle</value>
</property>
<property>
  <name>yarn.nodemanager.aux-services.mapreduce_shuffle.class</name>
  <value>org.apache.hadoop.mapred.ShuffleHandler</value>
</property>
<property>
  <name>yarn.nodemanager.vmem-pmem-ratio</name>
  <value>2.1</value>
</property>
<property>
  <name>yarn.nodemanager.resource.cpu-vcores</name>
  <value>11</value>
</property>
<property>
  <name>yarn.scheduler.minimum-allocation-mb</name>
  <value>2048</value>
</property>
<property>
  <name>yarn.scheduler.maximum-allocation-mb</name>
  <value>34359</value>
</property>
<property>
  <name>yarn.nodemanager.resource.memory-mb</name>
  <value>34359</value>
</property>
</configuration>" > $HADOOP_CONF_DIR/yarn-site.xml


#### Setup namenode, if this is the first time or not a persistent instance
echo "running hadoop namenode -format"
hdfs namenode -format


#### Start the Hadoop cluster
echo "Start Hadoop services (with mpirun) in the background"
echo "namenode starting on $HOSTNAME, logging to $HADOOP_LOG_DIR/namenode.log"
hdfs namenode > $HADOOP_LOG_DIR/namenode.log 2>&1 &
echo "datanode starting with mpirun, logging to $HADOOP_LOG_DIR/datanode.log"
mpirun --pernode bash -c "hdfs datanode > $HADOOP_LOG_DIR/datanode.log 2>&1" &
echo "resourcemanager starting on $HOSTNAME, logging to $HADOOP_LOG_DIR/resourcemanager.log"
yarn resourcemanager > $HADOOP_LOG_DIR/resourcemanager.log 2>&1 &
echo "nodemanager starting with mpirun, logging to $HADOOP_LOG_DIR/nodemanager.log"
mpirun --pernode bash -c "yarn nodemanager > $HADOOP_LOG_DIR/nodemanager.log 2>&1" &
## need to develop a reliable test for when services are ready...
sleep 10 # dodgy 10 s wait seems sufficient for services to be available
## the following is noot sufficient to test...
#echo "Wait until the services are ready with 'dfsadmin -safemode wait'"
#time hadoop dfsadmin -safemode wait
echo "listing background jobs - should be 4 just started and still running"
jobs

#### Run your jobs here
echo "Running VCF Clustererer...."

### Create a file with the nodes IP to connect to.... Probably a better way
time hadoop dfsadmin -report > debugfile

### Not required anymore
#export HADOOP_CLIENT_OPTS="-Xms16384m $HADOOP_CLIENT_OPTS"
#export HADOOP_CLIENT_OPTS="-Xmx32768m $HADOOP_CLIENT_OPTS"
#export MAHOUT_HEAPSIZE=32768
# check envvars which might override default args
#if [ "$MAHOUT_HEAPSIZE" != "" ]; then
#  echo "run with heapsize $MAHOUT_HEAPSIZE"
#  JAVA_HEAP_MAX="-Xmx""$MAHOUT_HEAPSIZE""m"
#  echo $JAVA_HEAP_MAX
#fi


#rm /data/noflush/obr17q/mini/user/obr17q/clustering/output/ -r

### Copy sequence files to DFS (rather than VCF -> CSV -> Sequence). Much faster.
hadoop dfs -Ddfs.block.size=134217728 -Dmapred.max.split.size=134217728 -copyFromLocal /data/noflush/obr17q/mini/user /

###Submit jar file.
yarn jar VCF-clusterer-0.0.1-SNAPSHOT.jar -libjars ${LIBJARS}




echo Job is finished!!!

### To copy sequencefiles/outputfiles from DFS
#hadoop dfs -copyToLocal / /data/noflush/obr17q/mini  

echo grab some info about HDFS just for the record
echo hdfs fsck /
time hdfs fsck /
echo hdfs dfsadmin -report
time hdfs dfsadmin -report

#### Stop the Hadoop cluster
echo "Stop the backgrounded Hadoop services"
jobs
kill `jobs -p`
wait

#### Save the logs in case there were problems to debug
mkdir $FLUSHDIR/hadoop_logs_$PBS_JOBID
echo logs being saved to $FLUSHDIR/hadoop_logs_$PBS_JOBID
mv $HADOOP_LOG_DIR/namenode.log $HADOOP_LOG_DIR/resourcemanager.log $FLUSHDIR/hadoop_logs_$PBS_JOBID/
for node in `uniq $PBS_NODEFILE`; do
  pbsdsh -c 1 -h $node mv $HADOOP_LOG_DIR/datanode.log $FLUSHDIR/hadoop_logs_$PBS_JOBID/datanode_$node.log
  pbsdsh -c 1 -h $node mv $HADOOP_LOG_DIR/nodemanager.log $FLUSHDIR/hadoop_logs_$PBS_JOBID/nodemanager$node.log
done

#### Clean up the working directories after job completion
echo "Clean up"
hpchadoop -x
rm -rf $HADOOP_CONF_DIR
