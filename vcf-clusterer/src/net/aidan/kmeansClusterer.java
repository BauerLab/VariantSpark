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
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.zip.GZIPInputStream;

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
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.hadoop.DistributedRowMatrix;
import org.apache.mahout.math.hadoop.stochasticsvd.SSVDSolver;
import org.apache.mahout.utils.vectors.RowIdJob;
import org.apache.mahout.clustering.canopy.CanopyDriver;
import org.apache.mahout.clustering.kmeans.Kluster;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.clustering.kmeans.RandomSeedGenerator;
import org.apache.mahout.clustering.streaming.mapreduce.StreamingKMeansDriver;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.common.distance.ManhattanDistanceMeasure;
import org.apache.mahout.math.hadoop.stochasticsvd.SSVDHelper;

import au.com.bytecode.opencsv.CSVReader;
public class kmeansClusterer extends Configured implements Tool {
	public kmeansClusterer() {}
	
	// Some variables     
	public static final String OUTPUT_DIRECTORY = "clustering";
	public static final boolean SINGLE_MACHINE = false;
	public static final int k = 10;
	public static long end = 0;
	public static long start = System.currentTimeMillis();
	public int run(String[] args) throws Exception {

		
		
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
		//transposeVCF();
		//mergeCSV();
		
		//Submits a job to Hadoop to read in the CSV file(s) and convert them to a Mahout sequence file
		csvToSequences(conf, fs);
		
		//Probably redundant because of next line (Or could be used for non-random centroids)
		//chooseInitialCentroids(conf, fs);
		
		//Create k centroids at random from the Mahout sequence file
		//Path centroids = RandomSeedGenerator.buildRandom(conf,new Path(OUTPUT_DIRECTORY + "/points/file1"),new Path(OUTPUT_DIRECTORY + "/clusters"),k,new ManhattanDistanceMeasure());
			
		//Submit the k-means clustering job to the cluster
		//System.out.println("Launching KMeansDriver");
		//KMeansDriver.run(conf, new Path(OUTPUT_DIRECTORY + "/points"),
		//		//new Path(OUTPUT_DIRECTORY + "/clusters"),
		//		centroids,
		//		new Path(OUTPUT_DIRECTORY + "/output"), 0.001, 1000, true, 0.001, SINGLE_MACHINE);


		
		/*
		 * Analysis stuff. Not Hadoop, can be run locally.
		 */
		
		//Writes clusters with their associated samples to "resultFileCluster.txt"
		//outputFormatter ob = new outputFormatter();
		//ob.fileRead(conf);
		
		//Matches samples to their pedigree and returns two arrays to find adjusted Rand index in Python scikit
		//pedigreeMatcher p = new pedigreeMatcher("20130606_g1k.ped");
		//p.findMatches("resultFileCluster.txt");
		//p.adjRandIndex("resultFileCluster.txt");

		
		//Other unrelated stuff
		//String[] args2 = new String[] {
		//		"-i",OUTPUT_DIRECTORY + "/points",
		//		"-o",OUTPUT_DIRECTORY + "/matrix"};
		//RowIdJob.main(args2);
		
		//DistributedRowMatrix m = new DistributedRowMatrix(new Path(OUTPUT_DIRECTORY + "/matrix"), new Path(OUTPUT_DIRECTORY + "/tmp"), 1092, 49970);
		//m.setConf(conf);
		//m.columnMeans();
		
		
		//Path[] path = new Path[1];
		//path[0] = new Path(OUTPUT_DIRECTORY + "/points");
		//int k2 = 2;
		//SSVDSolver ssvdsolver = new SSVDSolver(conf, path, new Path(OUTPUT_DIRECTORY + "/SSVTOUT"), 4, k2, 2, 3);
		//ssvdsolver.setOverwrite(true);
		//ssvdsolver.run();
		//Vector svalues = ssvdsolver.getSingularValues().viewPart(0, k2);
		//SSVDHelper.saveVector(svalues, new Path(OUTPUT_DIRECTORY + "/SSVTOUT/savedVector"), conf);
		

		
		System.out.println("Finished!!");
		end = System.currentTimeMillis();
		System.out.println("Time taken: " + (end - start) + " ms");
		return 0;
	}
	
	
	
	
	static void transposeVCF() throws IOException {
		System.out.println("Transposing file(s) to CSV format...");
		
		//Readers
		File folder = new File("vcf/");
		File[] listOfFiles = folder.listFiles();
		
		for (File s: listOfFiles) {
			InputStream fileStream = null;
			InputStream gzipStream = null;
			BufferedReader br;
			if (s.getName().contains(".vcf.gz")) {
				fileStream = new FileInputStream(s);
				gzipStream = new GZIPInputStream(fileStream);
				Reader decoder = new InputStreamReader(gzipStream, "UTF-8");
				br = new BufferedReader(decoder);
			} else if (s.getName().contains(".vcf")) {
				br = new BufferedReader(new FileReader(s));
			} else {
				continue;
			}
			String chromosome= s.getName().split("[.]")[1];
			System.out.print(chromosome + ", ");
			String sCurrentLine;
			int flag = 0;
			String[] splitted;
			String[] values;
			String[] samples = null;
		    BufferedWriter[] writers = null;
		    
			while ((sCurrentLine = br.readLine()) != null) {
				if (flag == 1){		
					splitted = sCurrentLine.split("	");
					values = Arrays.copyOfRange(splitted,9,splitted.length);
					ArrayList<String> items = new ArrayList<String>();
					for (int i = 0; i < values.length; i++) {
						if (values[i].split(":")[0].equals("0|0")) {
							items.add(",0");
							//writers[i].append(",0");
						} else if (values[i].split(":")[0].equals("0|1") || values[i].split(":")[0].equals("1|0")) {
							items.add(",1");
							//writers[i].append(",1");
						} else if (values[i].split(":")[0].equals("1|1")) {
							items.add(",2");
							//writers[i].append(",2");
						}	
					}				
				

				
				} else if (sCurrentLine.contains("#CHROM")) {
					splitted = sCurrentLine.split("	");
					samples = Arrays.copyOfRange(splitted,9,splitted.length);
					writers = new BufferedWriter[samples.length];
					
					
					new File("csv/" + chromosome).mkdirs();			  
					for (int i = 0; i < samples.length; i++) {
						writers[i] = new BufferedWriter(new FileWriter("csv/"+ chromosome +"/" + samples[i]));
						writers[i].write(samples[i]);
					}
					flag = 1;
				}
			}
			for (int i = 0; i < writers.length ; i++) {
				writers[i].close();
			}
			
			br.close();

			if (gzipStream != null) {
				fileStream.close();
				gzipStream.close();
			}

		}

	}
	static void mergeCSV() throws IOException {
		System.out.println("Merging VCF files...");
		//Readers
		File chrDirs = new File("csv/");
		File[] listOfChrDirs = chrDirs.listFiles(
			new FilenameFilter() {
			    public boolean accept(File dir, String name) {
			        return name.toLowerCase().contains("chr");
			    }
		});
		File chrFiles = new File("csv/"+listOfChrDirs[0].getName());
		File[] listOfChrFiles = chrFiles.listFiles(
			new FilenameFilter() {
			    public boolean accept(File dir, String name) {
			        return !name.contains(".");
			    }
		});	
		new File("csv/final").mkdir();	
		
		for (File s: listOfChrFiles) {
			BufferedWriter bw = new BufferedWriter(new FileWriter("csv/final/" + s.getName()));
			bw.write(s.getName());
			for (File t: listOfChrDirs) {
				BufferedReader br = new BufferedReader(new FileReader("csv/" + t.getName() + "/" + s.getName()));
				bw.write(br.readLine().substring(8));
				br.close();
			}			
			System.out.print("\rDone: " + s.getName());
			bw.close();
		}
	}
	
	
	static void csvToSequences(Configuration conf, FileSystem fs) throws IOException {
		System.out.println("Converting CSV input to Vector format...");
		VectorWritable vec = new VectorWritable();
		NamedVector individual;
		String sCurrentLine;
		
		//Readers
		File folder = new File("csv/final");
		File[] listOfFiles = folder.listFiles(new FilenameFilter() {
		    public boolean accept(File dir, String name) {
		        return !name.contains(".");
		    }});
				
		//Single writer
		Path path = new Path(OUTPUT_DIRECTORY + "/points/file1");
		SequenceFile.Writer writer = new SequenceFile.Writer(fs,  conf, path, Text.class, VectorWritable.class);

		//log
		BufferedWriter dasLog = new BufferedWriter(new FileWriter("deets"));
				
		int count = 0;
		for (File s: listOfFiles) {

			count++;
			BufferedReader br = new BufferedReader(new FileReader(s));
			sCurrentLine = br.readLine();

			//List<String> items = Arrays.asList(sCurrentLine.split("\\s*,\\s*"));
			
			System.out.println(count);
			System.out.println(s.getName());

			ArrayList<Double> arrLis = new ArrayList<Double>();
			
	        for (int i = 7; i < sCurrentLine.length(); i++) {
	        	String current = sCurrentLine.charAt(i)+ "";
	        	if (current.equals(",")){
	        		continue;
	        	}
	        	arrLis.add(Double.parseDouble(current));
	        }
	        
			String itemName = sCurrentLine.substring(0,7);
			dasLog.write( count + "\n" );

			int featuresSize = arrLis.size();
			System.out.println(featuresSize);	
			double[] features = new double[featuresSize];
						
			for(int indx = 0; indx < featuresSize ; indx++){
				features[indx] = arrLis.get(indx);
			}
			individual = new NamedVector(new SequentialAccessSparseVector(new DenseVector(features)), itemName );
			vec.set(individual);
			writer.append(new Text(individual.getName()), vec);
			br.close();
			dasLog.flush();
		}
		writer.close();
		dasLog.close();
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
