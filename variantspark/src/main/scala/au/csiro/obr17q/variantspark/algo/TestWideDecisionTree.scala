package au.csiro.obr17q.variantspark.algo

import au.csiro.obr17q.variantspark.SparkApp
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import au.csiro.obr17q.variantspark.metrics.Metrics

object TestWideDecisionTree extends SparkApp {
  conf.setAppName("VCF cluster")

  
  def main(args:Array[String]) {
    println("Testign WideKMeans")
    
    
    val dims = 1000
    val importantDims = 30
    val centersNo = 3
    val samples = 100
    val clusterVariance = 0.005
    val centers = sc.parallelize(Range(0,dims).map(i => Vectors.dense(Array.fill(centersNo)((Math.random()*3).toInt.toDouble))), 10)
    //centers.foreach(println)
    val clusterAssignment = Range(0,samples).map(i => Math.floor(Math.random()*centersNo).toInt).toList
    println(clusterAssignment)
    
    val vectorData:RDD[Vector] = centers.zipWithIndex().map{ case (v,i) =>
      if (i< importantDims) Vectors.dense(clusterAssignment.map(c =>
        ((v(c).toInt + (Math.random()*1.3).toInt) % centersNo).toDouble).toArray)
      else 
        Vectors.dense(Array.fill(samples)((Math.random()*3).toInt.toDouble))
    }
    
    
    val data = vectorData.zipWithIndex().cache()
    val testLabels = Range(0,samples).map(i => Math.floor(Math.random()*centersNo).toInt).toList
    val testData:RDD[Vector] = centers.zipWithIndex().map{ case (v,i) =>
      if (i< importantDims) Vectors.dense(testLabels.map(c =>
        ((v(c).toInt + (Math.random()*1.3).toInt) % centersNo).toDouble).toArray)
      else 
        Vectors.dense(Array.fill(samples)((Math.random()*3).toInt.toDouble))
    }    
    
    val test = data.count()
    println("Records to process: "+ test)
    
    val rf = new WideRandomForest()
    val result  = rf.run(data, clusterAssignment.toArray, 100)
    //println(result)
    //result.printout()
    val variableImportnace = result.variableImportance
    println(result.predict(data.map(_._1)).toList)    
    variableImportnace.toSeq.sortBy(-_._2).take(50).foreach(println)
    
    val testPredict = result.predict(testData)
    val testError = Metrics.classificatoinError(testLabels.toArray,testPredict)
    println(s"Test error: ${testError}")
    
    
  }
}