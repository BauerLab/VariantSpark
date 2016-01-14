package au.csiro.obr17q.variantspark

import au.csiro.obr17q.variantspark.model.CsvParser
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.classification.RandomForestClassificationModel
import org.apache.spark.ml.regression.RandomForestRegressionModel
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.ml.feature.{VectorIndexer, StringIndexer}
import org.apache.spark.rdd.RDD
import java.io._
/**
 * @author obr17q
 */
object VcfForest extends SparkApp {
  conf.setAppName("VCF foresting")

  def main(args: Array[String]) {

    val defaults = Array("/Users/obr17q/Desktop/coadread_mutations_bmi_filtered.csv","5","5","10","auto","0")


    //val seed = 3262362
    val VcfFiles = if (args.length > 0) args(0) else defaults(0)
    val NumTrees = if (args.length > 1) args(1).toInt else defaults(1).toInt
    //val maxDepth = args(2).toInt

    val FeatureSubsetStrategy = if (args.length > 4) args(4) else defaults(4)
    val VariantCutoff = if (args.length > 5) args(5).toInt else defaults(5).toInt


    // "f1", "precision", "recall", "weightedPrecision", "weightedRecall"
    val metricName = "f1"
    val numFolds = 8
    val labelName = "bmi_cat"
    val maxDepth = Array(5, 10)
    //val maxDepth = Array(5)
    val maxBins = Array(20)
    //val maxBins = Array(10)


    /**
      * TCGA settings
      * 01 - Individual ID
      * 06 - Sex
      * 46 - Weight
      * 47 - Height
      */
    //val PopFiles = Source.fromFile("data/nationwidechildrens.org_clinical_patient_coad.txt").getLines()
    //val IndividualMeta : RDD[IndividualMap] = sc.parallelize(new MetaDataParser(PopFiles, 3, '\t', "[Not Available]", IndividualIdCol = 1, PopulationCol = 2)(WeightCol = 46, HeightCol = 47, SexCol = 6).returnBmiMap())


    /**
      * TCGA settings
      * 00 - Individual
      * 01 - Population
      * 02 - Super Population
      * 03 - Gender
      */
    //val PopFiles = Source.fromFile("data/ALL.panel").getLines()
    //val IndividualMeta : RDD[IndividualMap] = sc.parallelize(new MetaDataParser(PopFiles, HeaderLines = 1, '\t', "", 0, 2 )(SexCol = 3).returnMap())


    //val modObject = new VcfParser(VcfFiles, VariantCutoff, IndividualMeta, sc)
    val modObject = new CsvParser(VcfFiles, VariantCutoff, sc)
    val data = modObject.data

    println(data.count)

    //val labelIndexer = new StringIndexer()
    //  .setInputCol(labelName)
    //  .setOutputCol("label")
    //  .fit(data)

    //val featureIndexer = new VectorIndexer()
    //  .setInputCol("preFeatures")
    //  .setOutputCol("features")
    //  .setMaxCategories(100)
    //  .fit(data)

    val rfClassifier = new RandomForestClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setNumTrees(NumTrees)
      .setFeatureSubsetStrategy(FeatureSubsetStrategy)

    val rfRegressor = new RandomForestRegressor()
      .setLabelCol("bmi")
      .setFeaturesCol("features")
      .setNumTrees(NumTrees)
      .setFeatureSubsetStrategy(FeatureSubsetStrategy)




    val pipeline = new Pipeline()
      //.setStages(Array(labelIndexer, rf))
      .setStages(Array(rfRegressor))

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("bmi")
      .setPredictionCol("prediction")
      .setMetricName(metricName)

    val paramGrid = new ParamGridBuilder()
      .addGrid(rfRegressor.maxDepth, maxDepth)
      .addGrid(rfRegressor.maxBins, maxBins)
      .build

    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(numFolds)

    //val model = pipeline.fit(trainingData)


    /**
      * Test the model on the test set.
      * Prints predictions and probabilities.
      */
    /*
    println("Output is:")
    cvModel.transform(testData)
      .select("individual", "sampleType", "label", "probability", "prediction")
      .collect()
      .foreach { case Row(individual: String, sampleType: String, label: Double, prob: Vector, prediction: Double) =>
        println(s"($individual, $sampleType, $label) --> prob=$prob, prediction=$prediction")
      }
    */

    val AlleleTuples = modObject.alleleTuples

    def modelFit(df: DataFrame): RDD[(Int, (String, Double))] = {
      val cvModel = cv.fit(df)

      val forestModel = cvModel
        .bestModel
        .asInstanceOf[PipelineModel]
        .stages(0 )
        .asInstanceOf[RandomForestRegressionModel]

      val importantFeatures = forestModel.featureImportances

      AlleleTuples
        .filter(p => importantFeatures(p._2) > 0.002)
        .map(p => (p._2, (p._1, importantFeatures(p._2))))
    }



    //val Array(trainingData, testData) = data.randomSplit(Array(0.8, 0.2))


    val f0, f1, f2, f3, f4 = modelFit(data)
    //val f5, f6, f7, f8, f9 = modelFit(data)
    //val f10, f11, f12, f13, f14 = modelFit(data)
    //val f15, f16, f17, f18, f19 = modelFit(data)
    //val f20, f21, f22, f23, f24 = modelFit(data)
    //val f25, f26, f27, f28, f29 = modelFit(data)
    //val f30, f31, f32, f33, f34 = modelFit(data)
    //val f35, f36, f37, f38, f39 = modelFit(data)
    //val f40, f41, f42, f43, f44 = modelFit(data)
    //val f45, f46, f47, f48, f49 = modelFit(data)


    //(19:16422392, 0.002136421712119211) (19:16422392, 0.004436421712119211)
    // (2, 19:16422392, 0.006)
    val allOfThem = (f0 union f1 union f2 union f3 union f4) /* union
                     f5 union f6 union f7 union f8 union f9 union
                     f10 union f11 union f12 union f13 union f14 union
                     f15 union f16 union f17 union f18 union f19 union
                     f20 union f21 union f22 union f23 union f24 union
                     f25 union f26 union f27 union f28 union f29 union
                     f30 union f31 union f32 union f33 union f34 union
                     f35 union f36 union f37 union f38 union f39 union
                     f40 union f41 union f42 union f43 union f44 union
                     f45 union f46 union f47 union f48 union f49 )*/
      .aggregateByKey((0, "", 0.0))((acc, value) => (acc._1 + 1, value._1, acc._3 + value._2), (acc1, acc2) => (acc1._1 + acc2._1, acc1._2, acc1._3 + acc2._3))





    allOfThem.map(p => (p._1, p._2._2, p._2._3, p._2._1)).sortBy(_._3).collect.foreach(println)






    //println("classes = " + forestModel.numClasses)
    //println("features = " + forestModel.numFeatures)
    //println("important features = " + filteredAlleleTuples.count)


    //model.transform(testData)
    //  .select("individual", "label", "prediction", "bmi")
    //  .collect()
    //  .foreach(p => println("%s (%s) (%s) predicted %scorrectly. BMI: %s" format(p(0), p(1), p(2), if (p(1).toString == p(2).toString) "" else "in", p(3) )))



    //val (trainingData, testData) = (splits(0), splits(1))
    
    
    
    /**
     * Print a count of healthy and obese individuals
     */
    
    //def isObese(i: Double) = i >= 30
    //def isMale(i: Double) = i == 1
    //println(IndividualVariants.count() + " individuals")
    //println("with " + NoOfAlleles + " alleles")
  
    


    
    /**
     * Strategies
     * For binary classification and regrssion
     */
    
    /*
    val ClassificationStrategy = new Strategy(algo = org.apache.spark.mllib.tree.configuration.Algo.Classification,
                                impurity = org.apache.spark.mllib.tree.impurity.Gini,
                                maxDepth = args(2).toInt,
                                numClasses = 3,
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

  */
  /**
   * Stuff for binary classifier
   */
    
    /*
  val TestArray: Array[Double] = new Array(10)
  val RandArray: Array[Double] = new Array(10)
    

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
    */
    
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