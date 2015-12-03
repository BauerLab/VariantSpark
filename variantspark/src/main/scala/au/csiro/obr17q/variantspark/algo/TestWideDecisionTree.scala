package au.csiro.obr17q.variantspark.algo

import au.csiro.obr17q.variantspark.SparkApp
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

object TestWideDecisionTree extends SparkApp {
  conf.setAppName("VCF cluster")

  
  def main(args:Array[String]) {
    println("Testign WideKMeans")
    
    
    val dims = 1000
    val centersNo = 3
    val samples = 30
    val clusterVariance = 0.005
    val centers = sc.parallelize(Range(0,dims).map(i => Vectors.dense(Array.fill(centersNo)((Math.random()*3).toInt.toDouble))), 10)
    //centers.foreach(println)
    val clusterAssignment = Range(0,samples).map(i => Math.floor(Math.random()*centersNo).toInt).toList
    println(clusterAssignment)
    
    val data:RDD[Vector] = centers.map(v =>
      Vectors.dense(clusterAssignment.map(c => ((v(c).toInt + (Math.random()*1.8).toInt) % centersNo).toDouble).toArray)
    )
    
    val test = data.cache().count()
    println("Records to process: "+ test)
    
    val rf = new WideDecisionTree()
    val result  = rf.run(data, clusterAssignment.toArray)
    //println(result)
    println(rf.predict(data, result).toList)
    
  }
}