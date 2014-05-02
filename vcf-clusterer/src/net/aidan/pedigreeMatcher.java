package net.aidan;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

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
	
	//Why??
	public void printPedigree() {
		Iterator i = hm.entrySet().iterator();
		while(i.hasNext()) {
	         Map.Entry me = (Map.Entry)i.next();
	         System.out.print(me.getKey() + ": ");
	         for (String s:(String[])me.getValue()) {
	        	 System.out.print(s+ " ");
	         }
	         System.out.print("\n");
	      }
	}
	
	public void findMatches(String clusteredPoints) throws IOException {
		BufferedWriter w = new BufferedWriter(new FileWriter("populations.txt"));
		BufferedReader ins =
				new BufferedReader(new FileReader(clusteredPoints));
		String line = null;
		while ((line = ins.readLine()) != null) {
			String[] samples = line.split(": ");
			w.write(line.split(":")[1] + ": ");
			String[] sampleArray = samples[2].split(",");
			for (String s:sampleArray){
				w.write(hm.get(s)[6]+",");				
			}
			w.write("\n");
		}
		w.close();
		ins.close();
	}
	
	public void adjRandIndex(String clusteredPoints) throws IOException {
		Iterator i = hm.entrySet().iterator();
		String truth = "truth=[";
		String clust = "clust=[";
		
		
		BufferedReader ins =
				new BufferedReader(new FileReader("populations.txt"));
		String line = null;
		int clusterId = 0;
		while ((line = ins.readLine()) != null) {
			
			String[] samples = line.split(":");
			String[] sampleArray = samples[1].split(",");
			
			for (String s:sampleArray){
				truth+=clusterId+",";
				if (s.contains("CHB")|s.contains("JPT")|s.contains("CHS")|s.contains("ASN")) {
					clust += 0+",";
					
				} else if (s.contains("CEU")|s.contains("TSI")|s.contains("FIN")|s.contains("GBR")|s.contains("IBS")|s.contains("EUR")) {
					clust += 1+",";
					
				} else if (s.contains("YRI")|s.contains("LWK")|s.contains("ASW")|s.contains("ACB")|s.contains("AFR")) {
					clust += 2+",";
					
				} else if (s.contains("MXL")|s.contains("PUR")|s.contains("CLM")|s.contains("AMR")) {
					clust += 3+",";
					
				}else {
					System.out.println(s);
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


