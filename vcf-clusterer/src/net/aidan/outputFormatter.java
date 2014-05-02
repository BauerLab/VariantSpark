package net.aidan;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.mahout.clustering.Cluster;
import org.apache.mahout.clustering.classify.WeightedPropertyVectorWritable;
import org.apache.mahout.math.NamedVector;

public class outputFormatter {
	String outputFile = "resultFileCluster.txt";
	LinkedHashMap<String,String> mpIdData = new LinkedHashMap<String, String>();
	
	public void fileRead(Configuration conf) {
		System.out.println("Writing summary to "+ outputFile);
		try {
			FileSystem fs = FileSystem.get(conf);
			Path outPath = new Path("clustering/output/" + Cluster.CLUSTERED_POINTS_DIR + "/");
			FileStatus[] status = fs.listStatus(outPath);
			
			IntWritable key = new IntWritable();
			WeightedPropertyVectorWritable value = new WeightedPropertyVectorWritable();
			
			for (FileStatus s: status) {
				Path newPath = s.getPath();
				if (!newPath.toString().contains("part-m-")){
					System.out.println(newPath);
					continue;
				}
				SequenceFile.Reader reader = new SequenceFile.Reader(fs, newPath, conf);
				while (reader.next(key, value)) {
					NamedVector vect = (NamedVector) value.getVector();
					String vecName = vect.getName();
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
