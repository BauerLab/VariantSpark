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
				if (s.contains("CHB")|s.contains("JPT")|s.contains("CHS")|s.contains("CDX")|s.contains("KHV")|s.contains("EAS")) {
					truth += 0+",";
					clust += clusterId+",";
					
				} else if (s.contains("CEU")|s.contains("TSI")|s.contains("FIN")|s.contains("GBR")|s.contains("IBS")|s.contains("EUR")) {
					truth += 1+",";
					clust += clusterId+",";
					
				} else if (s.contains("YRI")|s.contains("LWK")|s.contains("GWD")|s.contains("MSL")|s.contains("ESN")|s.contains("ASW")|s.contains("ACB")|s.contains("AFR")) {
					truth += 2+",";
					clust += clusterId+",";
					
				} else if (s.contains("MXL")|s.contains("PUR")|s.contains("CLM")|s.contains("PEL")|s.contains("AMR")) {
					truth += 3+",";
					clust += clusterId+",";
					
				} else if (s.contains("GIH")|s.contains("PJL")|s.contains("BEB")|s.contains("STU")|s.contains("ITU")|s.contains("SAS")) {
					truth += 4+",";
					clust += clusterId+",";
					
				} else {
					System.out.println("Unknown population: " + s);
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


