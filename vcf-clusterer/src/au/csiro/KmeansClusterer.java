/**
 * 
 */
/**
 * @author obr17q
 *
 */
package au.csiro;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.clustering.kmeans.RandomSeedGenerator;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;

public class KmeansClusterer extends Configured implements Tool {
	public KmeansClusterer() {}
	
	// Some variables     
	public static String INPUT_DIRECTORY = "transposed";
	public static String INTERMEDIATE_DIRECTORY ="clusters";
	public static String OUTPUT_DIRECTORY ="output";
	public static int k = 7;
	public static int iter = 20;
	
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
		String[] remainingArgs = optionParser.getRemainingArgs();
		FileSystem fs = FileSystem.get(conf);
		if(remainingArgs.length > 0){
			INPUT_DIRECTORY = remainingArgs[0] + "/transposed";
			INTERMEDIATE_DIRECTORY = remainingArgs[0] + "/clusters";
			OUTPUT_DIRECTORY = remainingArgs[0] +"/output";	
			k = Integer.parseInt(remainingArgs[1]);
			iter = Integer.parseInt(remainingArgs[4]);
		}
		conf.setInt("mapreduce.job.reduces", k);
		conf.set("yarn.app.mapreduce.am.resource.mb", "4096");
		conf.set("yarn.app.mapreduce.am.command-opts", "-Xmx3277M");
		
		conf.set("mapreduce.map.memory.mb","4096");
		conf.set("mapreduce.map.java.opts", "-Xmx3276m");
		conf.set("mapreduce.reduce.memory.mb", "4096");
		conf.set("mapreduce.reduce.java.opts", "-Xmx3276m");
		
		conf.set("mapreduce.task.io.sort.mb","256");
		conf.set("mapreduce.map.output.compress", "true");
		conf.set("mapreduce.map.output.compress.codec","org.apache.hadoop.io.compress.SnappyCodec");
		conf.set("mapreduce.output.fileoutputformat.compress","true");
		conf.set("mapreduce.output.fileoutputformat.compress.codec","org.apache.hadoop.io.compress.GzipCodec");
		conf.set("mapreduce.input.fileinputformat.split.minsize","134217728");

		
		
		
		Path centroids;
		// Use existing centroids if available (must have same feature size as input)
		if(fs.exists(new Path(INTERMEDIATE_DIRECTORY))){
			centroids = new Path(INTERMEDIATE_DIRECTORY);
		} else {
			centroids = RandomSeedGenerator.buildRandom(conf, new Path(INPUT_DIRECTORY), new Path(INTERMEDIATE_DIRECTORY), k, new EuclideanDistanceMeasure());
		}
				
		KMeansDriver.run(conf,
				new Path(INPUT_DIRECTORY),
				centroids,
        		new Path(OUTPUT_DIRECTORY),
				0.001, iter, true, 0.001, false);

		//Streaming k-means (not finished)
		//String[] args1 = new String[] {"-i", OUTPUT_DIRECTORY + "/points/", "-o", OUTPUT_DIRECTORY + "/output/" , "--estimatedNumMapClusters", "30", "--searchSize","2","-k","10", "--numBallKMeansRuns","3",  "--distanceMeasure","org.apache.mahout.common.distance.EuclideanDistanceMeasure"};
		//StreamingKMeansDriver.main(args1);


		return 0;
	}
	
	public static void main(String[] args) throws Exception {
	    int res = ToolRunner.run(new Configuration(), new KmeansClusterer(), args);
	    System.exit(res);
	  }
}
