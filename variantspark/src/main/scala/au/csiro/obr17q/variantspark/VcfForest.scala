package au.csiro.obr17q.variantspark

import java.util.Date

import au.csiro.obr17q.variantspark.CommonFunctions._
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.configuration.Strategy

import scala.io.Source

/**
 * @author obr17q
 */
object VcfForest extends SparkApp {
  conf.setAppName("VCF foresting")
  

  
  def main(args:Array[String]) {
    
    //val args = Array("data/smALL.vcf","5","5","100","auto","0.5")
    
    val seed = 4363465
    val VcfFiles = args(0)
    val VariantCutoff = args(5).toInt

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


    val vcfObject = new VcfParser(VcfFiles, VariantCutoff, sc)

    val NoOfAlleles = vcfObject.variantCount

    val FilteredAlleles = vcfObject.individualTuples



    val IndividualVariants = FilteredAlleles
    .groupByKey //group by individual ID, i.e. get RDD of individuals
    
    //.map(p => (p._1.split('_')(0).substring(0,12), (p._1.split('_')(1), p._2))) // TCGA data
    .map(p => (p._1, (p._1, p._2)))
    .join(IndividualMeta.map(_.toPops)) //filter out individuals lacking required data 
    .map(h =>
      
      (h._1, h._2._1._1, h._2._2, LabeledPoint(if (h._2._2 =="EUR") 0
                  else if (h._2._2 =="AFR") 1 else if (h._2._2 =="AMR") 2
                  else if (h._2._2 =="EAS") 3
                  else -1, Vectors.sparse(NoOfAlleles, h._2._1._2.to[Seq] ))) // Binary labels
      /*
      (h._1, h._2._1._1, h._2._2, LabeledPoint(if (h._2._2 =="GBR") 0
                  else if (h._2._2 =="ASW") 1 else if (h._2._2 =="CHB") 2
                  else -1, Vectors.sparse(NoOfAlleles, h._2._1._2.to[Seq] ))) // Binary labels
      */            
                  
      //(h._1, h._2._1._1, h._2._2, LabeledPoint(h._2._2, Vectors.sparse(NoOfAlleles, h._2._1._2.to[Seq] ) )
          
          
          
          
          
          
          //if (h._2._2 == 1) LabeledPoint(h._2._2, Vectors.sparse(NoOfAlleles, h._2._1._2.to[Seq] ))
          //else LabeledPoint(h._2._2, Vectors.sparse(NoOfAlleles, Seq((1,1.0), (2,1.0)) ))
          
      
      )  // Continuous labels
      

      
      
      
    //.filter(_._2 == "NORMAL")
    .cache
    //.map(h => (h._1, h._2, h._3, h._4.label)).collect().foreach(println)

    //val ReducedFeatureSelectedSet: RDD[(String, String, Double, LabeledPoint)] = sc
    //.objectFile("reducedset", 100)
    
    //val DataA = ReducedFeatureSelectedSet.map(p => (p._1, p._2, p._3, LabeledPoint(p._3, p._4.features)))
    
    


    
    val splits = IndividualVariants.randomSplit(Array(0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1), seed)
    
    //val (trainingData, testData) = (splits(0), splits(1))
    
    
    
    /**
     * Print a count of healthy and obese individuals
     */
    
    def isObese(i: Double) = i >= 30
    def isMale(i: Double) = i == 1
    println(IndividualVariants.count() + " individuals")
    println("with " + NoOfAlleles + " alleles")
  
    


    
    /**
     * Strategies
     * For binary classification and regrssion
     */
    

    val ClassificationStrategy = new Strategy(algo = org.apache.spark.mllib.tree.configuration.Algo.Classification,
                                impurity = org.apache.spark.mllib.tree.impurity.Gini,
                                maxDepth = args(2).toInt,
                                numClasses = 4,
                                maxBins = args(3).toInt,
                                categoricalFeaturesInfo = Map[Int, Int](),
                                maxMemoryInMB = 1024
                                )

    val RegressionStrategy = new Strategy(algo = org.apache.spark.mllib.tree.configuration.Algo.Regression,
                                impurity = org.apache.spark.mllib.tree.impurity.Variance,
                                maxDepth = args(2).toInt,
                                maxBins = args(3).toInt,
                                categoricalFeaturesInfo = Map[Int, Int]()
                               )  


  /**
   * Stuff for binary classifier
   */
    
    
  val TestArray: Array[Double] = new Array(10)
  val RandArray: Array[Double] = new Array(10)
    
  var a = 0
  for( a <- 0 to 9){
    val start = new Date().getTime
    val trainingData =
      if (a==0)       splits(1).union(splits(2)).union(splits(3)).union(splits(4)).union(splits(5)).union(splits(6)).union(splits(7)).union(splits(8)).union(splits(9))//0
      else if (a==1)  splits(0).union(splits(2)).union(splits(3)).union(splits(4)).union(splits(5)).union(splits(6)).union(splits(7)).union(splits(8)).union(splits(9))//1
      else if (a==2)  splits(0).union(splits(1)).union(splits(3)).union(splits(4)).union(splits(5)).union(splits(6)).union(splits(7)).union(splits(8)).union(splits(9))//2
      else if (a==3)  splits(0).union(splits(1)).union(splits(2)).union(splits(4)).union(splits(5)).union(splits(6)).union(splits(7)).union(splits(8)).union(splits(9))//3
      else if (a==4)  splits(0).union(splits(1)).union(splits(2)).union(splits(3)).union(splits(5)).union(splits(6)).union(splits(7)).union(splits(8)).union(splits(9))//3
      else if (a==5)  splits(0).union(splits(1)).union(splits(2)).union(splits(3)).union(splits(4)).union(splits(6)).union(splits(7)).union(splits(8)).union(splits(9))//3
      else if (a==6)  splits(0).union(splits(1)).union(splits(2)).union(splits(3)).union(splits(4)).union(splits(5)).union(splits(7)).union(splits(8)).union(splits(9))//3
      else if (a==7)  splits(0).union(splits(1)).union(splits(2)).union(splits(3)).union(splits(4)).union(splits(5)).union(splits(6)).union(splits(8)).union(splits(9))//3
      else if (a==8)  splits(0).union(splits(1)).union(splits(2)).union(splits(3)).union(splits(4)).union(splits(5)).union(splits(6)).union(splits(7)).union(splits(9))//3
      else            splits(0).union(splits(1)).union(splits(2)).union(splits(3)).union(splits(4)).union(splits(5)).union(splits(6)).union(splits(7)).union(splits(8))//4

    val testData = splits(a)
      
    val model = RandomForest.trainClassifier(
                                             input=trainingData.map(_._4),
                                             ClassificationStrategy,
                                             numTrees = args(1).toInt,
                                             featureSubsetStrategy = args(4),
                                             seed)

    val labelsAndPredictions = testData.map { point =>
      val prediction = model.predict(point._4.features)
      (point._1, point._2, point._4.label, prediction)
    }.map(p => (p._1, p._2, p._3, p._4))


    
                      
                                             
                                             
                                             

    /**
     * Stuff for regressor
     */
    /*
    val model = RandomForest.trainRegressor(
                                          input = trainingData.map(_._4),
                                          RegressionStrategy,
                                          numTrees = args(1).toInt,
                                          featureSubsetStrategy = args(4),
                                          seed)
*/





    // Evaluate model on test instances and compute test error

    
    //labelsAndPredictions.collect().foreach(println)

    
    println("Calculating metrics..")
    
    //// Adjusted Rand Index
    val resultArray: Array[(Double, Double)] = labelsAndPredictions.map(p => (p._3, p._4)).collect
    val clustered = "[%s]".format(resultArray.map(_._1.toString()).reduceLeft(_+","+_))
    val expected = "[%s]".format(resultArray.map(_._2.toString()).reduceLeft(_+","+_))
    val adjustedRandIndex = GetRandIndex(clustered, expected)
    


    //val testMSE = labelsAndPredictions.map(p => (p._3,p._4)).map{ case(v, p) => math.pow((v - p), 2)}.mean()
    val errors = labelsAndPredictions.filter(r => r._3 != r._4).count.toDouble
    val total = testData.count()
    val testErr = errors / total
    println(errors + " wrong out of " + total)
    println("Test Error = " + testErr)
    println("Adjusted Rand Index = " + adjustedRandIndex)
    //println("Learned classification forest model:\n" + model.toDebugString)

    val metrics = new MulticlassMetrics(labelsAndPredictions.map(p => (p._3, p._4)))
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