package au.csiro.obr17q.variantspark

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.{Vector=>MLVector, Vectors}
import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.lang.Math.sqrt
object CommonFunctions {
   //val logger = Logger.getLogger("au.csiro.obr17q.variantspark");
   //logger.setLevel(Warn)

  
  // Takes a VCF string and returns the distance between the variant and wildtype.
  // Returns m if variant is missing (i.e. .|.)
  // Useful to either impute 0 for missign variants, or -1 to filter out missing variants.
  def variantDist(n: String, m: Int): Double = {
    val alleles = n.split(":")(0)
    val distance = if(!alleles.matches("""^[0-9][\||\/][0-9]$""")) m else alleles.split("""[\|]|[/]""").map(_.toDouble).reduce(_+_)
    return distance
  }
  
  
  def numNonzeros(n: Array[Double]): Int = {
    // same as values.count(_ != 0.0) but faster
    var nnz = 0
    n.foreach { v =>
      if (v != 0.0) {
        nnz += 1
      }
    }
    return nnz
  }
    
}