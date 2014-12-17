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
import org.apache.hadoop.util.GenericOptionsParser;
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
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.common.distance.ManhattanDistanceMeasure;
import org.apache.mahout.math.hadoop.stochasticsvd.SSVDHelper;

public class KmeansClusterer extends Configured implements Tool {
	public KmeansClusterer() {}
	
	// Some variables     
	public static String INPUT_DIRECTORY;
	public static String INTERMEDIATE_DIRECTORY;
	public static String OUTPUT_DIRECTORY;
	public static int k;
	public static final int FEATURE_SIZE = 15161339;
	public static long end = 0;
	public static long start = System.currentTimeMillis();
	
	
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
		String[] remainingArgs = optionParser.getRemainingArgs();
		FileSystem fs = FileSystem.get(conf);
		INPUT_DIRECTORY = remainingArgs[0];	
		INTERMEDIATE_DIRECTORY = remainingArgs[1];		
		OUTPUT_DIRECTORY = remainingArgs[2];	
		k = Integer.parseInt(remainingArgs[3]);	
		conf.set("mapreduce.job.reduces", "40");
		
		Path centroids;
		// Remove the output dir if it already exists
		if(fs.exists(new Path(INTERMEDIATE_DIRECTORY))){
			centroids = new Path(INTERMEDIATE_DIRECTORY);
		} else {
			centroids = RandomSeedGenerator.buildRandom(conf, new Path(INPUT_DIRECTORY), new Path(INTERMEDIATE_DIRECTORY), k, new EuclideanDistanceMeasure());
		}
				
		//Probably redundant because of next line (Or could be used for non-random centroids)
		//chooseInitialCentroids(conf, fs);
		
		//end = System.currentTimeMillis();
		//System.out.println("Time taken: " + (end - start) + " ms");
		//Submit the k-means clustering job to the cluster
		//System.out.println("Launching KMeansDriver");
		
		//CanopyDriver.buildClusters(
		//		conf,
		//		new Path(INPUT_DIRECTORY),
		//		new Path(INTERMEDIATE_DIRECTORY),
		//		(DistanceMeasure) new EuclideanDistanceMeasure(),
		//		Double.parseDouble(remainingArgs[3]),
		//		Double.parseDouble(remainingArgs[4]),
		//		Integer.parseInt(remainingArgs[5]),
		//		false
		//		);
		
		KMeansDriver.run(conf,
				new Path(INPUT_DIRECTORY),
				centroids,
        		new Path(OUTPUT_DIRECTORY),
				0.001, 20, true, 0.001, false);

		//end = System.currentTimeMillis();
		//System.out.println("Time taken: " + (end - start) + " ms");
		//String[] args1 = new String[] {"-i", OUTPUT_DIRECTORY + "/points/", "-o", OUTPUT_DIRECTORY + "/output/" , "--estimatedNumMapClusters", "30", "--searchSize","2","-k","10", "--numBallKMeansRuns","3",  "--distanceMeasure","org.apache.mahout.common.distance.EuclideanDistanceMeasure"};
		//StreamingKMeansDriver.main(args1);


		return 0;
	}
	
	
	//Choose and write initial clusters	
	static void chooseInitialCentroids(Configuration conf,FileSystem fs) throws IOException {
		System.out.println("Creating initial centroids...");
		//VectorWritable vec = new VectorWritable();
		//Input CSVs
		File folder = new File("csv/final/");
		File[] listOfFiles = folder.listFiles();
		File[] centers = Arrays.copyOfRange(listOfFiles, 4, k+4);
		
		//Output file
		Path clusterPath = new Path(OUTPUT_DIRECTORY + "/clusters/part-00000");
		SequenceFile.Writer clusterWriter = new SequenceFile.Writer(fs, conf, clusterPath, Text.class, Kluster.class);
		
		//log
		BufferedWriter dasLog = new BufferedWriter(new FileWriter("deets"));
		
		String sCurrentLine = null;
		int centerID = 0;
		NamedVector centroid ;
		for (File s: centers) {	
			BufferedReader br = new BufferedReader(new FileReader(s));
			sCurrentLine = br.readLine();
			br.close();
			
			//String[] splitted = sCurrentLine.split(",");
			double[] test = new double[FEATURE_SIZE];
			
			int curVal = 0;
	        for (int i = 7; i < sCurrentLine.length(); i++) {
	        	String current = sCurrentLine.charAt(i)+ "";
	        	if (current.equals(",")){
	        		continue;
	        	}
	        	test[curVal] = Double.parseDouble(current);
	        	//arrLis.add(Double.parseDouble(current));
	        	curVal++;
	        }
			String itemName = sCurrentLine.substring(0,7);
			
			
			//String item_name = splitted[0];
			//int featuresSize = splitted.length-1;
			//double[] features = new double[featuresSize];
			//for(int indx = 1; indx <= featuresSize ; indx++){
			//	features[indx-1] = Float.parseFloat(splitted[indx]);
			//}
			
			System.out.println(itemName);
			centroid = new NamedVector(new SequentialAccessSparseVector(new DenseVector(test)), itemName );
			Kluster cluster = new Kluster(centroid, centerID, new ManhattanDistanceMeasure());
			clusterWriter.append(new Text(itemName), cluster);
			dasLog.write( centerID + "\n" );
			dasLog.flush();
			centerID++;
		}
		clusterWriter.close();
		dasLog.close();
	}
	
	
	
	public static void main(String[] args) throws Exception {
	    int res = ToolRunner.run(new Configuration(), new KmeansClusterer(), args);
	    System.exit(res);
	  }
}
