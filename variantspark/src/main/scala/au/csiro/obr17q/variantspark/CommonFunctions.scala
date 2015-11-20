package au.csiro.obr17q.variantspark

import org.apache.spark.rdd.RDD
import scala.language.postfixOps
import scala.sys.process._

object CommonFunctions {
   //val logger = Logger.getLogger("au.csiro.obr17q.variantspark");
   //logger.setLevel(Warn)


  def toCsv (processedRDD: RDD[String]) {
    
  }
  
  def variantDist(n: String, m: Int=0): Double = {
    val alleles = n.split(":")(0)
    val distance = if(!alleles.matches("""^[0-9][\||\/][0-9]$""")) m else alleles.split("""[\|]|[/]""").map(_.toDouble).reduce(_+_)
    return distance
  }
  
  def getBmi(height: Double, weight: Double): Double = {
    if (height > 0 && weight > 0) {
      val h = if (height > 10) height/100 else height
      weight / (h * h)
    } else 0
  }

  def GetRandIndex(predicted: String, expected: String, pythonPath: String = "/usr/bin/python"): String = {
    val pythonFunc = "from sklearn.metrics.cluster import adjusted_rand_score;print adjusted_rand_score(%s, %s)".format(predicted, expected)
    Seq(pythonPath, "-c", pythonFunc) !!
  }
  
}


