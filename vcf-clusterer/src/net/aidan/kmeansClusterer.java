/**
 * 
 */
/**
 * @author obr17q
 *
 */
package net.aidan;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.utils.vectors.RowIdJob;
import org.apache.mahout.clustering.canopy.CanopyDriver;
import org.apache.mahout.clustering.kmeans.Kluster;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.clustering.kmeans.RandomSeedGenerator;
import org.apache.mahout.clustering.streaming.mapreduce.StreamingKMeansDriver;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.common.distance.ManhattanDistanceMeasure;

public class kmeansClusterer extends Configured implements Tool {
	public kmeansClusterer() {}
	
	// Some variables
	public static final String INPUT_FILE =  "file.vcf";           
	public static final String OUTPUT_DIRECTORY = "clustering";
	public static final boolean SINGLE_MACHINE = false;
	public static final int k = 10;
	public static long end = 0;
	public int run(String[] args) throws Exception {
		long start = System.currentTimeMillis();
		
		
		Configuration conf = super.getConf();
		
		/*
		 * This adds the configuration files to 'conf'. Shouldn't have to do this.
		 * Stopped loading them automatically when I switched to Hadoop2.2
		 * It may work whithout these lines now, with other changes I made,
		 * but I haven't tested it yet. 
		 */
		final String HADOOP_CONF_DIR = System.getenv("HADOOP_CONF_DIR");
		conf.addResource(new Path(HADOOP_CONF_DIR, "core-site.xml"));
		conf.addResource(new Path(HADOOP_CONF_DIR, "hdfs-site.xml"));
		conf.addResource(new Path(HADOOP_CONF_DIR, "mapred-default.xml"));
		conf.addResource(new Path(HADOOP_CONF_DIR, "mapred-site.xml"));
		conf.addResource(new Path(HADOOP_CONF_DIR, "yarn-default.xml"));		
		conf.addResource(new Path(HADOOP_CONF_DIR, "yarn-site.xml"));
			
		
		/*
		 * Set the required parameters for Hadoop.
		 * "yarn" is essential for a non-sequential job
		 * Need to set number of reduces, but not mappers.
		 * Number of mappers seems to be sequenceFileSize/blocksize
		 * blocksize is 128mb by default, set by hpchadoop.
		 * Some other memory settings are in the vcf-clusterer.sh  
		 */
		conf.set("mapreduce.framework.name", "yarn");
		conf.set("mapreduce.job.reduces", "4");
		//conf.set("dfs.blocksize","134217728");
		
		conf.set("mapreduce.map.memory.mb", "3072");
		conf.set("mapreduce.map.java.opts", "-Xmx2458m");	
		
		conf.set("mapreduce.reduce.memory.mb", "4096");
		conf.set("mapreduce.reduce.java.opts", "-Xmx3277m");
				
		conf.set("yarn.app.mapreduce.am.resource.mb", "3072");
		conf.set("yarn.app.mapreduce.am.command-opts", "-Xmx2458m");

		conf.set("mapreduce.task.io.sort.mb","819");		

		
		FileSystem fs = FileSystem.get(conf);
		
		
		//Transpose a VCF file to CSV file(s)
		transposeVCF();
		
		
		//Submits a job to Hadoop to read in the CSV file(s) and convert them to a Mahout sequence file
		csvToSequences(conf, fs);
		
		//Probably redundant because of next line (Or could be used for non-random centroids)
		//chooseInitialCentroids(conf, fs);
		
		//Create k centroids at random from the Mahout sequence file
		Path centroids = RandomSeedGenerator.buildRandom(conf,new Path(OUTPUT_DIRECTORY + "/points/file0"),new Path(OUTPUT_DIRECTORY + "/clusters"),k,new ManhattanDistanceMeasure());
			
		//Submit the k-means clustering job to the cluster
		System.out.println("Launching KMeansDriver");
		KMeansDriver.run(conf, new Path(OUTPUT_DIRECTORY + "/points"),
				//new Path(OUTPUT_DIRECTORY + "/clusters"),
				centroids,
				new Path(OUTPUT_DIRECTORY + "/output"), 0.001, 1000, true, 0.001, SINGLE_MACHINE);


		
		/*
		 * Analysis stuff. Not Hadoop, can be run locally.
		 */
		
		//Writes clusters with their associated samples to "resultFileCluster.txt"
		outputFormatter ob = new outputFormatter();
		ob.fileRead(conf);
		
		//Matches samples to their pedigree and returns two arrays to find adjusted Rand index in Python scikit
		pedigreeMatcher p = new pedigreeMatcher("20130606_g1k.ped");
		p.findMatches("resultFileCluster.txt");
		p.adjRandIndex("resultFileCluster.txt");

		//Other unrelated stuff
		//StreamingKMeansDriver.main(das);
		//String[] args1 = new String[] {"-i","/home/name/workspace/XXXXX-vectors/tfidf-vectors","-o","/home/name/workspace/XXXXX-vectors/tfidf-vectors/SKM-Main-result/","--estimatedNumMapClusters","200","--searchSize","2","-k","12", "--numBallKMeansRuns","3",  "--distanceMeasure","org.apache.mahout.common.distance.CosineDistanceMeasure"};
		//RowIdJob.main(args);
		
		
		System.out.println("Finished!!");
		end = System.currentTimeMillis();
		System.out.println("Time taken: " + (end - start) + " ms");
		return 0;
	}
	
	
	
	
	static String[] transposeVCF() throws IOException {
		System.out.println("Transposing "+ INPUT_FILE + " to CSV format...");
		BufferedReader br = new BufferedReader(new FileReader(INPUT_FILE));
		String sCurrentLine;
		int flag = 0;
		String[] splitted;
		String[] samples = null;
	    BufferedWriter[] writers = null;
	    
		while ((sCurrentLine = br.readLine()) != null) {
	    //for (int line = 0; line < 200; line++) {
	    	//sCurrentLine = br.readLine();
			if (flag == 1){		
				splitted = sCurrentLine.split("	");
				String[] values = Arrays.copyOfRange(splitted,9,splitted.length);
				for (int i = 0; i < values.length; i++) {
					if (values[i].split(":")[0].equals("0|0")) {
						writers[i].append(",0");
					} else if (values[i].split(":")[0].equals("0|1")) {
						writers[i].append(",1");
					} else if (values[i].split(":")[0].equals("1|0")) {
						writers[i].append(",1");
					} else if (values[i].split(":")[0].equals("1|1")) {
						writers[i].append(",2");
					}
				}				
				
			} else if (sCurrentLine.contains("#CHROM")) {
				splitted = sCurrentLine.split("	");
				samples = Arrays.copyOfRange(splitted,9,splitted.length);
				writers = new BufferedWriter[samples.length];
				for (int i = 0; i < samples.length; i++) {
					writers[i] = new BufferedWriter(new FileWriter("csv/" + samples[i]));
					writers[i].write(samples[i]);
				}
				flag = 1;
			}
		}
		for (int i = 0; i < writers.length ; i++) {
			writers[i].close();
		}
		br.close();
		return samples;
	}
	
	
	
	static void csvToSequences(Configuration conf, FileSystem fs) throws IOException {
		System.out.println("Converting CSV input to Vector format...");
		VectorWritable vec = new VectorWritable();
		NamedVector individual;
		String sCurrentLine;
		
		//Readers
		File folder = new File("csv/");
		File[] listOfFiles = folder.listFiles();
				
		//Single writer
		Path path = new Path(OUTPUT_DIRECTORY + "/points/file1");
		SequenceFile.Writer writer = new SequenceFile.Writer(fs,  conf, path, Text.class, VectorWritable.class);
		
		//Many writers
		//int splits = 1;
		//Path[] path = new Path[splits];
		//SequenceFile.Writer[] writer = new SequenceFile.Writer[splits];
		//for (int split=0;split<splits;split++){
		//	path[split] = new Path(OUTPUT_DIRECTORY + "/points/file" + split);
		//	writer[split] = new SequenceFile.Writer(fs,  conf, path[split], Text.class, VectorWritable.class);
		//} 
		//int noOfSamples = folder.listFiles().length;
		//int distributions = (noOfSamples/splits)+1; 
		//int i = 0;		
		
		
		for (File s: listOfFiles) {
			BufferedReader br = new BufferedReader(new FileReader(s));
			sCurrentLine = br.readLine();
			String[] splitted = sCurrentLine.split(",");
			String item_name = splitted[0];
			if (item_name.length() == 0) {
				continue;
			}
			int featuresSize = splitted.length-1;
			double[] features = new double[featuresSize];
			for(int indx = 1; indx <= featuresSize ; indx++){
				features[indx-1] = Float.parseFloat(splitted[indx]);
			}
			
			individual = new NamedVector(new SequentialAccessSparseVector(new DenseVector(features)), item_name );
			vec.set(individual);
			writer.append(new Text(individual.getName()), vec);
			//writer[i/distributions].append(new Text(individual.getName()), vec);
			br.close();
			//i++;
		}
		
		//Close the Writer

		
		//Single writer
		writer.close();
		
		//Many writers
		//for (int split=0;split<splits;split++){
		//	writer[split].close();
		//}
	}
	
	
	//Choose and write initial clusters	
	static void chooseInitialCentroids(Configuration conf,FileSystem fs) throws IOException {
		System.out.println("Creating initial centroids...");
		VectorWritable vec = new VectorWritable();
		//Input CSVs
		File folder = new File("csv/");
		File[] listOfFiles = folder.listFiles();
		File[] centers = Arrays.copyOfRange(listOfFiles, 4, k+4);
		
		//Output file
		Path clusterPath = new Path(OUTPUT_DIRECTORY + "/clusters/part-00000");
		SequenceFile.Writer clusterWriter = new SequenceFile.Writer(fs, conf, clusterPath, Text.class, Kluster.class);
		
		String sCurrentLine = null;
		int i = 0;
		NamedVector centroid ;
		for (File s: centers) {
			
			BufferedReader br = new BufferedReader(new FileReader(s));
			sCurrentLine = br.readLine();
			br.close();
			
			String[] splitted = sCurrentLine.split(",");
			String item_name = splitted[0];
			int featuresSize = splitted.length-1;
			double[] features = new double[featuresSize];
			for(int indx = 1; indx <= featuresSize ; indx++){
				features[indx-1] = Float.parseFloat(splitted[indx]);
			}
			System.out.println(item_name);
			System.out.println(features[0]);
			centroid = new NamedVector(new SequentialAccessSparseVector(new DenseVector(features)), item_name );
			Kluster cluster = new Kluster(centroid, i, new ManhattanDistanceMeasure());
			clusterWriter.append(new Text(item_name), cluster);
			i++;
			System.out.println(i);
		}
		clusterWriter.close();
	}
	
	
	
	public static void main(String[] args) throws Exception {
	    int res = ToolRunner.run(new Configuration(), new kmeansClusterer(), args);
	    System.exit(res);
	  }
}
