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
    val maxBins = Array(20, 40)
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



    //val Array(trainingData0, testData0) = data.randomSplit(Array(0.8, 0.2))
    //val Array(trainingData1, testData1) = data.randomSplit(Array(0.8, 0.2))
    //val Array(trainingData2, testData2) = data.randomSplit(Array(0.8, 0.2))
    //val Array(trainingData3, testData3) = data.randomSplit(Array(0.8, 0.2))
    //val Array(trainingData4, testData4) = data.randomSplit(Array(0.8, 0.2))
    //val Array(trainingData5, testData5) = data.randomSplit(Array(0.8, 0.2))
    //val Array(trainingData6, testData6) = data.randomSplit(Array(0.8, 0.2))
    //val Array(trainingData7, testData7) = data.randomSplit(Array(0.8, 0.2))
    //val Array(trainingData8, testData8) = data.randomSplit(Array(0.8, 0.2))
    //val Array(trainingData9, testData9) = data.randomSplit(Array(0.8, 0.2))

    val filteredAlleleTuples0 = modelFit(data)
    val filteredAlleleTuples1 = modelFit(data)
    val filteredAlleleTuples2 = modelFit(data)
    val filteredAlleleTuples3 = modelFit(data)
    val filteredAlleleTuples4 = modelFit(data)
    /*val filteredAlleleTuples5 = modelFit(data)
    val filteredAlleleTuples6 = modelFit(data)
    val filteredAlleleTuples7 = modelFit(data)
    val filteredAlleleTuples8 = modelFit(data)
    val filteredAlleleTuples9 = modelFit(data)
    val filteredAlleleTuples10 = modelFit(data)
    val filteredAlleleTuples11 = modelFit(data)
    val filteredAlleleTuples12 = modelFit(data)
    val filteredAlleleTuples13 = modelFit(data)
    val filteredAlleleTuples14 = modelFit(data)
    val filteredAlleleTuples15 = modelFit(data)
    val filteredAlleleTuples16 = modelFit(data)
    val filteredAlleleTuples17 = modelFit(data)
    val filteredAlleleTuples18 = modelFit(data)
    val filteredAlleleTuples19 = modelFit(data)
    val filteredAlleleTuples20 = modelFit(data)
    val filteredAlleleTuples21 = modelFit(data)
    val filteredAlleleTuples22 = modelFit(data)
    val filteredAlleleTuples23 = modelFit(data)
    val filteredAlleleTuples24 = modelFit(data)
    val filteredAlleleTuples25 = modelFit(data)
    val filteredAlleleTuples26 = modelFit(data)
    val filteredAlleleTuples27 = modelFit(data)
    val filteredAlleleTuples28 = modelFit(data)
    val filteredAlleleTuples29 = modelFit(data)
    val filteredAlleleTuples30 = modelFit(data)
    val filteredAlleleTuples31 = modelFit(data)
    val filteredAlleleTuples32 = modelFit(data)
    val filteredAlleleTuples33 = modelFit(data)
    val filteredAlleleTuples34 = modelFit(data)
    val filteredAlleleTuples35 = modelFit(data)
    val filteredAlleleTuples36 = modelFit(data)
    val filteredAlleleTuples37 = modelFit(data)
    val filteredAlleleTuples38 = modelFit(data)
    val filteredAlleleTuples39 = modelFit(data)
    val filteredAlleleTuples40 = modelFit(data)
    val filteredAlleleTuples41 = modelFit(data)
    val filteredAlleleTuples42 = modelFit(data)
    val filteredAlleleTuples43 = modelFit(data)
    val filteredAlleleTuples44 = modelFit(data)
    val filteredAlleleTuples45 = modelFit(data)
    val filteredAlleleTuples46 = modelFit(data)
    val filteredAlleleTuples47 = modelFit(data)
    val filteredAlleleTuples48 = modelFit(data)
    val filteredAlleleTuples49 = modelFit(data)*/

    //val filteredAlleleTuples9 = modelFit(trainingData9)

    //(19:16422392, 0.002136421712119211) (19:16422392, 0.004436421712119211)
    // (2, 19:16422392, 0.006)
    val allOfThem = (filteredAlleleTuples0 union filteredAlleleTuples1 union filteredAlleleTuples2 union filteredAlleleTuples3 union filteredAlleleTuples4) /* union
                     filteredAlleleTuples5 union filteredAlleleTuples6 union filteredAlleleTuples7 union filteredAlleleTuples8 union filteredAlleleTuples9 union
                     filteredAlleleTuples10 union filteredAlleleTuples11 union filteredAlleleTuples12 union filteredAlleleTuples13 union filteredAlleleTuples14 union
                     filteredAlleleTuples15 union filteredAlleleTuples16 union filteredAlleleTuples17 union filteredAlleleTuples18 union filteredAlleleTuples19 union
                     filteredAlleleTuples20 union filteredAlleleTuples21 union filteredAlleleTuples22 union filteredAlleleTuples23 union filteredAlleleTuples24 union
                     filteredAlleleTuples25 union filteredAlleleTuples26 union filteredAlleleTuples27 union filteredAlleleTuples28 union filteredAlleleTuples29 union
                     filteredAlleleTuples30 union filteredAlleleTuples31 union filteredAlleleTuples32 union filteredAlleleTuples33 union filteredAlleleTuples34 union
                     filteredAlleleTuples35 union filteredAlleleTuples36 union filteredAlleleTuples37 union filteredAlleleTuples38 union filteredAlleleTuples39 union
                     filteredAlleleTuples40 union filteredAlleleTuples41 union filteredAlleleTuples42 union filteredAlleleTuples43 union filteredAlleleTuples44 union
                     filteredAlleleTuples45 union filteredAlleleTuples46 union filteredAlleleTuples47 union filteredAlleleTuples48 union filteredAlleleTuples49 )*/
    //val allOfThem = (filteredAlleleTuples0  union filteredAlleleTuples1)
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