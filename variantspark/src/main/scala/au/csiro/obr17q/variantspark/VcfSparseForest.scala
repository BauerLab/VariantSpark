package au.csiro.obr17q.variantspark

import java.util.Date
import au.csiro.obr17q.variantspark.CommonFunctions._
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.configuration.Strategy
import scala.io.Source
import au.csiro.obr17q.variantspark.metrics.Metrics

/**
 * @author obr17q
 */
object VcfSparseForest extends SparkApp {
  conf.setAppName("VCF foresting")
   
  
  val superPopToId = Map(("EUR",0), ("AFR",1), ("AMR",2), ("EAS",3))
  
  def main(args:Array[String]) {
    
    //val args = Array("data/smALL.vcf","5","5","100","auto","0.5")
    
    val seed = 4363465
    val inputFiles = args(0)
    val numberOfTrees  = args(1).toInt
    val featureSubsetStrategy = args(2)
    val maxDepths = args(3).toInt
    val maxBins = args(4).toInt

    /**
     * TCGA settings
     * 01 - Individual ID
     * 06 - Sex
     * 46 - Weight
     * 47 - Height
     */
    //val PopFiles = Source.fromFile("data/nationwidechildrens.org_clinical_patient_coad.txt").getLines()
    //val IndividualMeta = sc.parallelize(new MetaDataParser(PopFiles, 3, '\t', "[Not Available]", 1, 2 )(WeightCol = 46, HeightCol = 47, SexCol = 6).returnSexMap())

    
    /**
     * TCGA settings
     * 00 - Individual
     * 01 - Population
     * 02 - Super Population
     * 03 - Gender
     */
    val PopFiles = Source.fromFile("data/ALL.panel").getLines()
    val IndividualMeta = sc.parallelize(new MetaDataParser(PopFiles, HeaderLines = 1, '\t', "", 0, 2 )(SexCol = 3).returnMap())

    println(IndividualMeta.collect().toList)  
    

    val individualToSuperId = IndividualMeta
      .map(i=> (i.IndividualId,superPopToId.getOrElse(i.PopulationId,-1))).collectAsMap()
    
      
    println(individualToSuperId)
      
    val sparseVariat = sqlContext.read.parquet(inputFiles)
    
    val br_individualToSuperId = sc.broadcast(individualToSuperId)
    
    val individualVariants = 
      sparseVariat.rdd
        .filter(r => br_individualToSuperId.value.contains(r.getString(0)))
        .map(r=> (r.getString(0), Vectors.sparse(r.getInt(1),
          r.getSeq[Int](2).toArray, r.getSeq[Double](3).toArray)))
        .map{ case (individualId, sparseVariants) => (individualId, 
              new LabeledPoint(br_individualToSuperId.value(individualId).toDouble
              , sparseVariants))}.cache()
  
    val variantCount =   individualVariants.count()
    println("Running for variants: " + variantCount)
              
    /**
     * Strategies
     * For binary classification and regrssion
     */
    
   
    val ClassificationStrategy = new Strategy(algo = org.apache.spark.mllib.tree.configuration.Algo.Classification,
                                impurity = org.apache.spark.mllib.tree.impurity.Gini,
                                maxDepth = maxDepths,
                                numClasses = 4,
                                maxBins = maxBins,
                                categoricalFeaturesInfo = Map[Int, Int](),
                                maxMemoryInMB = 1024
                                )

    val RegressionStrategy = new Strategy(algo = org.apache.spark.mllib.tree.configuration.Algo.Regression,
                                impurity = org.apache.spark.mllib.tree.impurity.Variance,
                                maxDepth = maxDepths,
                                maxBins = maxBins,
                                categoricalFeaturesInfo = Map[Int, Int]()
                               )  


  /**
   * Stuff for binary classifier
   */
    
    
  val TestArray: Array[Double] = new Array(10)
  val RandArray: Array[Double] = new Array(10)
     
  // forget the splits just use random resampling
  for( a <- 0 to 9){
    val start = new Date().getTime
   
    val split = individualVariants.randomSplit(Array(0.75, 0.25), a)
    val trainingData = split(0)
    val testData = split(1)
  
      
      
      
    val model = RandomForest.trainClassifier(
                                             input=trainingData.map(_._2),
                                             ClassificationStrategy,
                                             numTrees = numberOfTrees,
                                             featureSubsetStrategy = featureSubsetStrategy,
                                             seed)

    val labelsAndPredictions = testData.map { point =>
      val prediction = model.predict(point._2.features)
      (point._1,  point._2.label, prediction)
    }

                                 
    println("Calculating metrics..")
    
    //// Adjusted Rand Index
    val resultArray: Array[(Int, Int)] = labelsAndPredictions.map(p => (p._2.toInt, p._3.toInt)).collect
    
    val unzipped = resultArray.unzip
    val adjustedRandIndex = Metrics.adjustedRandIndex(unzipped._1.toList, unzipped._2.toList)
    
    //val testMSE = labelsAndPredictions.map(p => (p._3,p._4)).map{ case(v, p) => math.pow((v - p), 2)}.mean()
    val errors = labelsAndPredictions.filter(r => r._2 != r._3).count.toDouble
    val total = testData.count()
    val testErr = errors / total
    println(errors + " wrong out of " + total)
    println("Test Error = " + testErr)
    println("Adjusted Rand Index = " + adjustedRandIndex)
    //println("Learned classification forest model:\n" + model.toDebugString)

    val metrics = new MulticlassMetrics(labelsAndPredictions.map(p => (p._2, p._3)))
    val precision = Array(metrics.precision(0), metrics.precision(1), metrics.precision(2))
    val recall = Array(metrics.recall(0), metrics.recall(1), metrics.recall(2))
    

    println(metrics.confusionMatrix.toString())
    
    
    
    println("GBR - precision:" + precision(0) + " recall:" + recall(0))
    println("ASW - precision:" + precision(1) + " recall:" + recall(1))
    println("CHB - precision:" + precision(2) + " recall:" + recall(2))
    //println("EAS - precision:" + precision(3) + " recall:" + recall(3))
    
    
    TestArray(a) = testErr
    RandArray(a) = adjustedRandIndex.toDouble
    val end = new Date().getTime
    println("Job took "+(end-start)/1000 + " seconds")

    //val metrics = new MulticlassMetrics(labelsAndPredictions.map(p => (p._3, p._4)))
    //val precision = Array(metrics.precision(0), metrics.precision(1), metrics.precision(2), metrics.precision(3))
    //val recall = Array(metrics.recall(0), metrics.recall(1), metrics.recall(2), metrics.recall(3))
    }  
    //println("EUR - precision:" + precision(0) + " recall:" + recall(0))
    //println("AFR - precision:" + precision(1) + " recall:" + recall(1))
    //println("AMR - precision:" + precision(2) + " recall:" + recall(2))
    //println("EAS - precision:" + precision(3) + " recall:" + recall(3))

  
  
  
    println("Test errors: " + TestArray.mkString(", "))
    println("Adjusted Rand Indices: " + RandArray.mkString(", "))
    
    
    /*
    val selector = new ChiSqSelector(50000)
    val transformer = selector.fit(SparseVariants.map(_._4))
    val filteredData = SparseVariants.map { lp => 
      LabeledPoint(lp._4.label, transformer.transform(lp._4.features)) 
    }

    val featuresThatAreSelected = transformer.selectedFeatures.map(a => print(a, ","))




    */
    
    
    
  }
}