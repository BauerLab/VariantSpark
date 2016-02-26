package au.csiro.obr17q.variantspark

import au.csiro.obr17q.variantspark.model.{ThousandGenomesVcfParser, CsvParser, VcfParser, ABetaParser}
import org.apache.spark.ml.classification.{RandomForestClassifier, RandomForestClassificationModel}
import org.apache.spark.ml.regression.{RandomForestRegressor, RandomForestRegressionModel}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{VectorIndexer, StringIndexer}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.rdd.RDD
import scala.io._

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
    val metricName = "weightedPrecision"

    val numFolds = 5
    val numModels = 1
    val labelName = "aBeta"
    //val maxDepth = Array(5, 10)
    val maxDepth = Array(5)
    val maxBins = Array(10)
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
      * 1000 Genomes Project settings
      * 00 - Individual
      * 01 - Population
      * 02 - Super Population
      * 03 - Gender
      */
    val PopFiles = Source.fromFile("data/ALL.panel").getLines()
    val IndividualMeta : RDD[IndividualMap] = sc.parallelize(new MetaDataParser(PopFiles, HeaderLines = 1, '\t', "", 0, 1 )(SexCol = 3, SuperPopulationCol = 2).returnMap())


    //val modObject = new ThousandGenomesVcfParser(VcfFiles, VariantCutoff, IndividualMeta, sc, sqlContext)
    //val modObject = new CsvParser(VcfFiles, VariantCutoff, sc, sqlContext)
    val modObject = new ABetaParser(VcfFiles, sc, sqlContext)


    val data = modObject.data
    val Array(trainingData, testData) = data.randomSplit(Array(0.6, 0.4))
    val total = data.count
    println("Samples", total)





    val labelIndexer = new StringIndexer()
      .setInputCol(labelName)
      .setOutputCol("label")
      .fit(data)

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
      .setMaxBins(5)
      .setMaxDepth(5)

    //val rfRegressor = new RandomForestRegressor()
    //  .setLabelCol("label")
    //  .setFeaturesCol("features")
    //  .setNumTrees(NumTrees)
    //  .setFeatureSubsetStrategy(FeatureSubsetStrategy)


    val pipeline = new Pipeline()
      .setStages(Array(labelIndexer, rfClassifier))
      //.setStages(Array(rfClassifier))

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName(metricName)

    val paramGrid = new ParamGridBuilder()
      .addGrid(rfClassifier.maxDepth, maxDepth)
      .addGrid(rfClassifier.maxBins, maxBins)
      .build

    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(numFolds)





    /**
      * Prints predictions and probabilities from fitted DF.
      */
    def printPredictions(df: DataFrame) = {
      println("Predictions:")
      df.collect
        .foreach { case Row(individual: String, aBeta: String, label: Double, prediction: Double) =>
          println(s"($individual, $aBeta)", label==prediction)
        }
    }

    /**
      * Prints important features from given model.
      */
    def printFeatures(rf: RandomForestClassificationModel) = {
      println("Important features:")
      val importantFeatures = rf.featureImportances
      val featureTuples = sc.parallelize(modObject.featureTuples)
      featureTuples
        .filter(p => importantFeatures(p._2) > 0.002)
        .map(p => (p._2, (p._1, importantFeatures(p._2))))
        .sortBy(_._2._2)
        .collect.foreach(println)
    }



    def modelFit(trainDF: DataFrame, testDF: DataFrame) = {
      // Build the cross-validated model
      val cvModel = cv.fit(trainDF)

      // Pull out the best Random Forest model
      val forestModel = cvModel
        .bestModel
        .asInstanceOf[PipelineModel]
        .stages(1)
        .asInstanceOf[RandomForestClassificationModel]

      // Get the size of the test DataFrame
      val total: Int = testDF.count.toInt

      // Make predictions on the test DataFrame
      val predictions: DataFrame = cvModel.transform(testDF).select("individual", "aBeta", "label", "prediction")

      // Get RDD of predictions and labels
      val predictionsAndLabels = predictions.select("prediction", "label")
        .map(row => (row.getDouble(0), row.getDouble(1)))

      // Get metrics from above RDD
      val metrics = new MulticlassMetrics(predictionsAndLabels)

      // Print metrics/predictions/features/etc.
      printPredictions(predictions)
      printFeatures(forestModel)
      println(metrics.confusionMatrix)
      println("Samples", total)
      println("Correct", (metrics.precision*total).toInt)
      println("Precision", metrics.precision)
      println("Recall", metrics.recall)
    }






    modelFit(trainingData, testData)








    /**
      * Creates n models, gets the feature importances and counts their ocurences.
      */
    //Array.fill[RDD[(Int, (String, Double))]](numModels)(modelFit(data))
    //  .reduce(_ union _)
    //  .aggregateByKey(0, "", 0.0)((acc, value) => (acc._1 + 1, value._1, acc._3 + value._2), (acc1, acc2) => (acc1._1 + acc2._1, acc1._2, acc1._3 + acc2._3))
    // .map(p => (p._1, p._2._2, p._2._3, p._2._1)).sortBy(_._3).collect.foreach(println)






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