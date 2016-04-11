package au.csiro.obr17q.variantspark

import au.csiro.obr17q.variantspark.model._
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.regression.{RandomForestRegressionModel, RandomForestRegressor}
import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.ml.evaluation.{MulticlassClassificationEvaluator, RegressionEvaluator}
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.mllib.evaluation.{MulticlassMetrics, RegressionMetrics}
import org.apache.spark.mllib.linalg._
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer
import scala.io._
import scala.math._

/**
  * @author obr17q
  */

object VcfForest extends SparkApp {
  conf.setAppName("VCF foresting")

  def main(args: Array[String]) {

    val defaults = Array("/Users/obr17q/Desktop/coadread_mutations_bmi_filtered.csv","5","5,10","10","auto","false","simulationTwo","0","10", "100","1000")

    //val seed = 3262362

    val VcfFiles = if (args.length > 0) args(0) else defaults(0)
    val NumTrees = if (args.length > 1) args(1).toInt else defaults(1).toInt
    val maxDepth = if (args.length > 2) args(2).split(",").map(_.toInt) else defaults(2).split(",").map(_.toInt)
    val maxBins = if (args.length > 3) args(3).split(",").map(_.toInt) else defaults(3).split(",").map(_.toInt)
    val FeatureSubsetStrategy = if (args.length > 4) args(4) else defaults(4)
    val classification = if (args.length > 5) args(5).toBoolean else defaults(5).toBoolean
    val labelName = if (args.length > 6) args(6) else defaults(6)
    val VariantCutoff = if (args.length > 7) args(7).toInt else defaults(7).toInt
    val numFolds = if (args.length > 8) args(8).toInt else defaults(8).toInt

    println(s"Parameters: Trees=$NumTrees, " +
      s"MaxDepth(s)=[${maxDepth.head}], " +
      s"MaxBins=[${maxBins.head}], " +
      s"nTry=$FeatureSubsetStrategy")

    println(s"Running ${if (classification) "classification" else "regression"} using label=$labelName")

    //Feature Indexing stuff
    val indexFeatures = true
    val maxCategories = 10


    val syntheticSamples = if (args.length > 7) args(7).toInt else defaults(7).toInt
    val syntheticFeatures = if (args.length > 8) args(8).toInt else defaults(8).toInt
    // Metric used for determining best cross-validation model
    // "f1", "precision", "recall", "weightedPrecision", "weightedRecall"
    val metricName = "mse"

    // Can be used for building many models
    val numModels = 1


    val label = if (classification) "label" else labelName
    val features = if (indexFeatures) "indexedFeatures" else "features"

    /**
      * TCGA settings
      * 01 - Individual ID
      * 06 - Sex
      * 46 - Weight
      * 47 - Height
      */
    //val PopFiles = "data/nationwidechildrens.org_clinical_patient_coad.txt"
    //val IndividualMeta : RDD[IndividualMap] = sc.parallelize(
    //  new MetaDataParser
    //  (Source.fromFile(PopFiles).getLines(), HeaderLines = 3, '\t', "[Not Available]", IndividualIdCol = 1, PopulationCol = 2)
    //  (WeightCol = 46, HeightCol = 47, SexCol = 6).returnBmiMap()
    //)


    /**
      * 1000 Genomes Project settings
      * 00 - Individual
      * 01 - Population
      * 02 - Super Population
      * 03 - Gender
      */
    //val PopFiles = "data/ALL.panel"
    //lazy val IndividualMeta : RDD[IndividualMap] = sc.parallelize(
    //  new MetaDataParser
    //  (Source.fromFile(PopFiles).getLines(), HeaderLines = 1, '\t', "", IndividualIdCol = 0, PopulationCol = 1 )
    //  (SexCol = 3, SuperPopulationCol = 2).returnMap()
    //)


    //val modObject = new ThousandGenomesVcfParser(VcfFiles, VariantCutoff, IndividualMeta, sc, sqlContext)
    val modObject = new CsvParser(VcfFiles, VariantCutoff, sc, sqlContext)
    //val modObject = new ABetaParser(VcfFiles, sc, sqlContext)
    //val modObject = new FakeDataParser(samples = syntheticSamples, dims = syntheticFeatures, save = false, sc, sqlContext)


    val data = modObject.data.cache

    //modObject.saveAs("100x1000.csv")

    val Array(trainingData, testData) = data.randomSplit(Array(0.8, 0.2))


    //val gender = modObject.feature("col5")
    //val a = data.map(_.getAs[SparseVector](2)(gender+10)).distinct.count
    //val a = data.map( row => (row.getAs[Double](labelName), row.getAs[Vector]("features").toArray)).take(5)
    //val b = a(0)
    //val c = a(1)
    //println(b._2.length)
    //println(b._1 - 10 * sin(10 * Pi * b._2.head))
    //println(c._1 - 10 * sin(10 * Pi * c._2.head))




    val labelIndexer = new StringIndexer()
      .setInputCol(labelName)
      .setOutputCol("label")
      .fit(data)




    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(maxCategories)
      .fit(data)


    //Classification

    val rfClassifier = new RandomForestClassifier()
      .setLabelCol("label")
      .setFeaturesCol(features)
      .setNumTrees(NumTrees)
      .setFeatureSubsetStrategy(FeatureSubsetStrategy)
      //.setImpurity("entropy")


    //Regression

    val rfRegressor = new RandomForestRegressor()
      .setLabelCol(labelName)
      .setFeaturesCol(features)
      .setNumTrees(NumTrees)
      .setFeatureSubsetStrategy(FeatureSubsetStrategy)
      //.setImpurity("entropy")



    // Convert indexed labels back to original labels.
    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)



    val tasks = new ListBuffer[PipelineStage]()


    if (classification) tasks += labelIndexer
    if (indexFeatures) tasks += featureIndexer
    if (classification) tasks += rfClassifier else tasks += rfRegressor
    if (classification) tasks += labelConverter
    //Array(labelIndexer, featureIndexer, rfClassifier, labelConverter)

    // The index of the Random Forest classifier or regressor
    val rfIndex = Array(tasks.toList.indexOf(rfClassifier),tasks.toList.indexOf(rfRegressor)).filter(_ != -1).head

    val pipeline = new Pipeline()
      .setStages(tasks.toArray)

    val regressionEvaluator = new RegressionEvaluator()
      .setLabelCol(label)
      .setPredictionCol("prediction")
      .setMetricName(metricName)

    //val classificationEvaluator = new MulticlassClassificationEvaluator()
    //  .setLabelCol(label)
    //  .setPredictionCol("prediction")
    //  .setMetricName(metricName)

    val paramGrid = new ParamGridBuilder()
      .addGrid(rfClassifier.maxDepth, maxDepth)
      .addGrid(rfClassifier.maxBins, maxBins)
      .build

    //val cv = new CrossValidator()
    //  .setEstimator(pipeline)
    //  .setEvaluator(regressionEvaluator)
    //  .setEstimatorParamMaps(paramGrid)
    //  .setNumFolds(numFolds)


    /**
      * Prints predictions and probabilities from fitted DF.
      */
    def printPredictions(df: DataFrame) = {
      println("Predictions:")
      df.collect
        .foreach { case Row(individual: String, aBeta: Double, label: Double, prediction: Double) =>
          println(s"($individual, $aBeta)", prediction, label == prediction)
        }
    }

    /**
      * Prints important features from given model.
      */
    def printFeatures(rf: Vector) = {
      println("Important features:")
      val featureTuples = sc.parallelize(modObject.featureTuples)
      featureTuples
        .map(p => (p._2, (p._1, rf(p._2))))
        .sortBy(_._2._2, ascending = false)
        .take(100)
        .foreach(println)
    }

    def modelFit(trainDF: DataFrame, testDF: DataFrame) = {

      val testSize: Int = testDF.count.toInt
      println(s"Total samples: ${data.count}")
      println(s"Testset: $testSize")


      // Build the cross-validated model
      val cvModel = pipeline.fit(trainDF)


      // Pull out the best Random Forest model
      val untypedForestModel = cvModel
        //.bestModel
        //.asInstanceOf[PipelineModel]
        .stages(rfIndex)




      // Make predictions on the test DataFrame
      val predictions: DataFrame = cvModel.transform(testDF).select("individual", label, label, "prediction")

      // Get RDD of predictions and labels
      val predictionsAndLabels = predictions.select("prediction", label)
        .map(row => (row.getDouble(0), row.getDouble(1)))

      // Get metrics from above RDD
      val metrics = new RegressionMetrics(predictionsAndLabels)

      // Print metrics/predictions/features/etc.
      printPredictions(predictions)
      //println(metrics.confusionMatrix)
      println(s"Samples: $testSize")
      //println(s"Correct: ${(metrics.precision*testSize).toInt}")
      println(s"MSE: ${metrics.meanSquaredError}")
      println(s"MAE: ${metrics.meanAbsoluteError}")
      println(s"R2: ${metrics.r2}")


      val importantFeatures = if (classification)
        untypedForestModel.asInstanceOf[RandomForestClassificationModel].featureImportances
        else untypedForestModel.asInstanceOf[RandomForestRegressionModel].featureImportances

      //val featureTuples = sc.parallelize(modObject.featureTuples)
      //featureTuples
      //  .map(p => (p._2, (p._1, importantFeatures(p._2))))
      //  .sortBy(_._2._2, ascending = false).take(50)

      printFeatures(importantFeatures)


    }






    modelFit(data, data)








    /**
      * Creates n models, gets the feature importances and counts their ocurences.
      */
    //Array.fill[RDD[(Int, (String, Double))]](numModels)(modelFit(data, data))
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