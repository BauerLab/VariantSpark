/**
 * 
 */
/**
 * @author obr17q
 *
 */
package au.csiro;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.VectorWritable;



public class VcfToSeq extends Configured implements Tool {
	//public csvClusterer() {}
	
	// Some variables
	public static String[] idArray;
	public static String SEQUENCE_OUT_DIRECTORY = "clustering";
	public static String INPUT_DIRECTORY;
	public static final boolean SINGLE_MACHINE = false;
	public static final int k = 50;
	public static final int FEATURE_SIZE = 15161339;
	public static long end = 0;
	public static long start = System.currentTimeMillis();
	private static final Log log = LogFactory.getLog(VcfToSeq.class);

		
	public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, VectorWritable>{
		
		private Text individual_id = new Text();
		private VectorWritable genotype = new VectorWritable();
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			SequentialAccessSparseVector sparseGeno = new SequentialAccessSparseVector(1100000000);

			log.info("Tokenizing string");
			String[] itr = value.toString().split(",");
			log.info("String split successfully");
			individual_id.set(itr[0]);
			int n = itr.length-1;
			String token;
			
			for (int i = 0  ; i < n; i++) {
				token = itr[i+1];
				sparseGeno.set(i, Double.parseDouble(token));
			}
			log.info("Writing sample data");
			genotype.set(sparseGeno);
			context.write(individual_id, genotype);
		}
	}
	

	public static class vcfMapper extends Mapper<LongWritable, Text, IntIntComposite, DoubleWritable>{
		private IntIntComposite individualId_location = new IntIntComposite();
		private DoubleWritable variant = new DoubleWritable();
		private int location;
		public int individualId;

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String variantString;
			String[] alleles;
			double variantDouble;
			String line = value.toString();
			if (String.valueOf(line.charAt(0)).equals("#")){
				return;
			}

			String[] itr = line.split("\\s+");
			
			//int chrId = Integer.parseInt(itr[0]+"0000000")-10000000;
			//location = chrId+ (int) (key.get()/10000);
			location = (int) (key.get()/10000);

			int l = itr.length;
			for (int i = 9  ; i < l; i++) {
				variantString = itr[i];
				alleles = variantString.split("\\|");
				variantDouble = Math.sqrt(Double.parseDouble(alleles[0])+Double.parseDouble(alleles[1]));
				
				if (variantDouble != 0) {
					variant.set(variantDouble);
					individualId = i-9;
					individualId_location.set(individualId, location);
					context.write(individualId_location, variant);
				}
			}
		}
	}
	
	public static class vcfReducer extends Reducer<IntIntComposite, DoubleWritable, Text, VectorWritable>{
		
		private Text newKey = new Text();
		private VectorWritable genotype = new VectorWritable();
				
		public void reduce(IntIntComposite key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
			newKey.set(  key.getIndividualId().toString()  );
			SequentialAccessSparseVector sparseGeno = new SequentialAccessSparseVector(3200000);

			for (DoubleWritable val : values) {
				sparseGeno.set(key.getVariantLocation().get(), val.get());
			}
			genotype.set(sparseGeno);
			context.write(newKey, genotype);
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = this.getConf();
		GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
		String[] remainingArgs = optionParser.getRemainingArgs();
		FileSystem fs = FileSystem.get(conf);

		// For testing locally..
		if(remainingArgs.length > 0){
			INPUT_DIRECTORY = remainingArgs[0];
			SEQUENCE_OUT_DIRECTORY = remainingArgs[1];
		} else {
			INPUT_DIRECTORY = "vcf/sample.vcf";
		}
		
		// Remove the output dir if it already exists
		if(fs.exists(new Path(SEQUENCE_OUT_DIRECTORY))){
			fs.delete(new Path(SEQUENCE_OUT_DIRECTORY),true);
		}	
		
		// Set up Hadoop job parameters and start it
	    Job job = Job.getInstance(conf, "VCF to sequence converter");
	    job.setJarByClass(VcfToSeq.class);
	    job.setMapperClass(vcfMapper.class);
	    job.setPartitionerClass(IndividualKeyPartitioner.class);
	    job.setGroupingComparatorClass(IndividualKeyGroupingComparator.class);
	    job.setSortComparatorClass(LocationKeyComparator.class);
	    job.setReducerClass(vcfReducer.class);
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(SequenceFileOutputFormat.class);
	    job.setMapOutputKeyClass(IntIntComposite.class);
	    job.setMapOutputValueClass(DoubleWritable.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(VectorWritable.class);
	    FileInputFormat.addInputPath(job, new Path(INPUT_DIRECTORY));
	    FileOutputFormat.setOutputPath(job, new Path(SEQUENCE_OUT_DIRECTORY));
	    return job.waitForCompletion(true) ? 0 : 1;
	 		
	}
	
	public static void main(String[] args) throws Exception {
	    int res = ToolRunner.run(new Configuration(), new VcfToSeq(), args);
	    System.exit(res);
	  }
	
}
