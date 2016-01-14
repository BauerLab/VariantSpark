package au.csiro;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class pedigreeMatcher {
	HashMap<String, String[]> hm = new HashMap<String, String[]>();
	
	public pedigreeMatcher(String pedFile) throws IOException {
		BufferedReader ins =
				new BufferedReader(new FileReader(pedFile));
		String line = null;
		while ((line = ins.readLine()) != null) {
			String[] splitted = line.split("	");
			hm.put(splitted[1],splitted);
		}
		ins.close();
	}

	public enum Phase1Pops {
		EAS("JPT", "CHB", "CHS"), SAS(), EUR("CEU", "TSI", "FIN", "GBR", "IBS"), 
		AFR("YRI", "LWK", "ASW"), AMR("MXL", "PUR", "CLM");
		private final List<String> values;
		Phase1Pops(String ...values) {
			this.values = Arrays.asList(values);
		}
	    public List<String> getValues() {
	        return values;
	    }
	}
	public enum Phase3Pops {
		EAS("CHB", "JPT", "CHS", "CDX", "KHV"),	SAS("GIH", "PJL", "BEB", "STU", "ITU"),	EUR("CEU", "TSI", "FIN", "GBR", "IBS"), 
		AFR("YRI", "LWK", "GWD", "MSL", "ESN", "ASW", "ACB"), AMR("MXL", "PUR", "CLM", "PEL");
		private final List<String> values;
		Phase3Pops(String ...values) {
			this.values = Arrays.asList(values);
		}
	    public List<String> getValues() {
	        return values;
	    }
	}
	public void findMatches(Configuration conf, String jobID) throws IOException {
		FileSystem fs = FileSystem.get(conf);
	    Path popPt=new Path(jobID + "/populations.txt");
	    Path resPt=new Path(jobID + "/resultFileCluster.txt");
		BufferedWriter w =
				new BufferedWriter(new OutputStreamWriter(fs.create(popPt, true)));
		BufferedReader r =
				new BufferedReader(new InputStreamReader(fs.open(resPt)));
				
		String line = null;
		while ((line = r.readLine()) != null) {
			String[] samples = line.split(": ");
			w.write(line.split(":")[1] + ": ");
			String[] sampleArray = samples[2].split(",");
			for (String s:sampleArray){
				w.write(hm.get(s)[6]+",");				
			}
			w.write("\n");
		}
		w.close();
		r.close();
	}

	public void adjRandIndex(Configuration conf, String jobID, String phase) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		String truth = "truth=[";
		String clust = "clust=[";
		
	    Path pt=new Path(jobID + "/populations.txt");
	    BufferedReader ins=new BufferedReader(new InputStreamReader(fs.open(pt)));
		String line = null;
		int clusterId = 0;
		


		
		
		while ((line = ins.readLine()) != null) {
			
			String[] samples = line.split(":");
			String[] sampleArray = samples[1].split(",");
			Phase3Pops superPop = null;        
			for (String s:sampleArray){
				
			    for (Phase3Pops pop : Phase3Pops.values()) {
			        if (pop.getValues().contains(s.trim())) {
			            superPop = pop;
			        }
			    }
			    
				switch (superPop) {
				case EAS:
					truth += 0+",";
					clust += clusterId+",";
					break;
				case SAS:
					truth += 1+",";
					clust += clusterId+",";
					break;
				case EUR:
					truth += 2+",";
					clust += clusterId+",";
					break;
				case AFR:
					truth += 3+",";
					clust += clusterId+",";
					break;
				case AMR:
					truth += 4+",";
					clust += clusterId+",";
					break;
				default:
					System.out.println("Other");
					break;
				}	
				
				
			}
			clusterId +=1;
		}
		truth += "]";
		clust += "]";

		System.out.println( truth );
		System.out.println( clust );
	}
	
}


