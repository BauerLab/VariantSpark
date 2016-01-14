package au.csiro;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class GetResults extends Configured implements Tool {
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
		String[] remainingArgs = optionParser.getRemainingArgs();
		String phase = "phase1";
		String sample_names = "data/phase3.txt";
		String job_id = ".";
		
		int mode = 0;
		
		if (remainingArgs.length > 0) {
			job_id = remainingArgs[0];
		}
		
		if (remainingArgs.length > 1) {
			mode = Integer.parseInt(remainingArgs[1]);
		}
		if (remainingArgs.length > 2) {
			phase = remainingArgs[2];
			sample_names = "data/" + phase;
		}	
		
		//Writes clusters with their associated samples to "resultFileCluster.txt"
		switch (mode) {
		case 1:
			sequenceToText(conf, job_id, sample_names);
			break;
		case 2:
			textToRandArray(conf, job_id, phase);
			break;
		default:
			sequenceToText(conf, job_id, sample_names);
			textToRandArray(conf, job_id, phase);
			break;		
		}

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
		return 0;
	}

	private static void sequenceToText(Configuration conf, String input, String sample_names) {
		outputFormatter of = new outputFormatter();
		of.fileRead(conf, input, sample_names);
	}
	
	private static void textToRandArray(Configuration conf, String jobId, String phase) throws IOException {
		System.out.println("Printing arrays for ");
		System.out.println("\n\n\n");
		
		pedigreeMatcher p = new pedigreeMatcher("data/integrated_call_samples.20130502.ALL.ped");
		p.findMatches(conf, jobId);
		p.adjRandIndex(conf, jobId, phase);
		
		System.out.println("from sklearn import metrics");
		System.out.println("metrics.adjusted_rand_score(truth, clust)");
		System.out.println("\n\n\n");
	}	
	
	
	
	
	public static void main(String[] args) throws Exception {
	    int res = ToolRunner.run(new Configuration(), new GetResults(), args);
	    System.exit(res);
	  }
}
