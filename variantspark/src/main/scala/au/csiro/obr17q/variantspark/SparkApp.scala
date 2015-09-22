package au.csiro.obr17q.variantspark

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level
//import org.bdgenomics.adam.rdd.ADAMContext

trait SparkApp {
  def defaultMasterUrl = "local"
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  
  lazy val conf = new SparkConf()
  //.set("spark.shuffle.memoryFraction", "0.2")
  //.set("spark.storage.memoryFraction", "0.4")
  .set("spark.yarn.executor.memoryOverhead", "2048")
  .set("spark.driver.maxResultSize","2048")
  .set("spark.default.parallelism", "256")
  lazy val masterUrl =  if (System.getenv("MASTER") != null) System.getenv("MASTER") else defaultMasterUrl
  lazy val sc = { println("MASTER: " + masterUrl) ; new SparkContext(masterUrl,this.getClass().getName(),conf)}
  //lazy val ac = new ADAMContext(sc)
}