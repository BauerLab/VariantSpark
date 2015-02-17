package au.csiro;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.mahout.clustering.Cluster;
import org.apache.mahout.clustering.classify.WeightedPropertyVectorWritable;
import org.apache.mahout.clustering.classify.WeightedVectorWritable;
import org.apache.mahout.math.NamedVector;

public class outputFormatter {
	String outputFile = "resultFileCluster.txt";
	LinkedHashMap<String,String> mpIdData = new LinkedHashMap<String, String>();
	
	public void fileRead(Configuration conf, String path, String namesFile) {
		System.out.println("Writing summary to "+ outputFile + "...");
		try {
			FileSystem fs = FileSystem.get(conf);
			Path inPath = new Path(path);
			FileStatus[] status = fs.listStatus(inPath);
			
			IntWritable key = new IntWritable();
			WeightedPropertyVectorWritable value = new WeightedPropertyVectorWritable();
			
			String content = new Scanner(new File(namesFile)).useDelimiter("\\Z").next();
			String[] sampleNames = content.split(",");
			
			System.out.println("Number of individuals: " + sampleNames.length);
			
			
			
			for (FileStatus s: status) {
				Path newPath = s.getPath();
				if (!newPath.toString().contains("part-m-")){
					continue;
				}
				System.out.println(newPath);
				SequenceFile.Reader reader = new SequenceFile.Reader(fs, newPath, conf);
				while (reader.next(key, value)) {
					NamedVector vect = (NamedVector) value.getVector();
					

					//String vecName = vect.getName();
					String vecName= sampleNames[Integer.parseInt(vect.getName())];

					String clusterName = key.toString();

					//Create a new element or add to existing element.
					if (mpIdData.containsKey(clusterName)) {
						String val = mpIdData.get(clusterName) + "," + vecName;
						mpIdData.put(clusterName, val);
					} else {
						mpIdData.put(clusterName, vecName);
					}				
				}
				reader.close();
			}
			FileWriter f1 = new FileWriter(outputFile,true);
			BufferedWriter out = new BufferedWriter(f1);
			for(Map.Entry<String, String> entry : mpIdData.entrySet()) {
				out.write("Cluster Id : " + entry.getKey());
				out.write("-------> Samples : " + entry.getValue());
				out.write("\n");
			}
			out.close();
		} catch(Exception e) {
			String message = getStackTrace(e);
			System.out.println(message);
		}
	}

	public static String getStackTrace(final Throwable throwable) {
		final StringWriter sw = new StringWriter();
		final PrintWriter pw = new PrintWriter(sw, true);
		throwable.printStackTrace(pw);
		return sw.getBuffer().toString();
	}
}
