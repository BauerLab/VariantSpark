package au.csiro.obr17q.variantspark

/**
 * @author obr17q
 */
object TcgaClustering extends SparkApp{
  
  def main(args:Array[String]) {
    val args1 = {
      if (masterUrl == "local") {
        /*
         * Define offline variables here!!!
         */
        Array(
            "data/data.vcf", //Input VCF file
            "0", "2811814",  //Filter for variants between these two values
            "3",             //Number of clusters (k)
            "GBR,ASW,CHB",   //Groups for inclusion
            "0.1"            //Sample size (0 - 1)
        )
      } else {
      args
      }
    }
    if (args1.length < 1) {
        println("Usage: CsvClusterer <input-path>")
    }
    
    
    
    val VcfFiles = sc.textFile(args1(0))
    
    
    
    
    
    
    
  }
  
  
  
  
}