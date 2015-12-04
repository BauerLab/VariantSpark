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
    val importantDims = 10
    val centersNo = 3
    val samples = 30
    val clusterVariance = 0.005
    val centers = sc.parallelize(Range(0,dims).map(i => Vectors.dense(Array.fill(centersNo)((Math.random()*3).toInt.toDouble))), 10)
    //centers.foreach(println)
    val clusterAssignment = Range(0,samples).map(i => Math.floor(Math.random()*centersNo).toInt).toList
    println(clusterAssignment)
    
    val data:RDD[Vector] = centers.zipWithIndex().map{ case (v,i) =>
      if (i< importantDims) Vectors.dense(clusterAssignment.map(c =>
        ((v(c).toInt + (Math.random()*1.1).toInt) % centersNo).toDouble).toArray)
      else 
        Vectors.dense(Array.fill(samples)((Math.random()*3).toInt.toDouble))
    }
    
    val test = data.cache().count()
    println("Records to process: "+ test)
    
    val rf = new WideRandomForest()
    val result  = rf.run(data, clusterAssignment.toArray, 50)
    //println(result)
    result.printout()
    val variableImportnace = result.variableImportance
    
    variableImportnace.toSeq.sortBy(-_._2).take(50).foreach(println)
    
    
    //println(rf.predict(data, result).toList)
    
  }
}