package au.csiro.obr17q.variantspark

import au.csiro.obr17q.variantspark.model.{ThousandGenomesVcfParser, CsvParser, VcfParser, ABetaParser}
import org.apache.spark.ml.{PipelineModel, Pipeline}
import org.apache.spark.ml.classification.{LogisticRegressionModel, LogisticRegression}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.tuning.{CrossValidatorModel, ParamGridBuilder, CrossValidator}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.sql.{DataFrame, Row}

/**
  * @author obr17q
  */

object VcfLogRegression extends SparkApp {
  conf.setAppName("VCF logistic regression")
  
  def main(args: Array[String]) {
    val defaults = Array("/Users/obr17q/Desktop/coadread_mutations_bmi_filtered.csv","0")
    // "f1", "precision", "recall", "weightedPrecision", "weightedRecall"
    val metricName = "f1"
    val numFolds = 5
    val labelName = "aBeta"
    val maxIter = Array(10)
    val regParam = Array(0.01)

    val VcfFiles = if (args.length > 0) args(0) else defaults(0)

    val VariantCutoff = if (args.length > 1) args(1).toInt else defaults(1).toInt

    /**
     * TCGA settings
     * 00 - Individual
     * 01 - Population
     * 02 - Super Population
     * 03 - Gender
     */
    //val PopFiles = Source.fromFile("data/ALL.panel").getLines()
    //val IndividualMeta = sc.parallelize(new MetaDataParser(PopFiles, HeaderLines = 1, '\t', "", 0, 1 )(SexCol = 3).returnMap(IncludeGroups = Array("GBR","ASW", "CHB")))
    //val IndividualMeta = sc.parallelize(new MetaDataParser(PopFiles, HeaderLines = 1, '\t', "", 0, 2 )(SexCol = 3).returnMap())

    //val modObject = new ThousandGenomesVcfParser(VcfFiles, VariantCutoff, IndividualMeta, sc, sqlContext)
    //val modObject = new CsvParser(VcfFiles, VariantCutoff, sc, sqlContext)
    val modObject = new ABetaParser(VcfFiles, sc, sqlContext)

    //val NoOfAlleles = modObject.variantCount
    //val FilteredAlleles = modObject.individualTuples

    val data = modObject.data
    val Array(trainingData, testData) = data.randomSplit(Array(0.8, 0.2))
    val total = data.count
    println("Samples", total)



    val labelIndexer = new StringIndexer()
      .setInputCol(labelName)
      .setOutputCol("label")
      .fit(data)

    val lrRegressor = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.01)

    val pipeline = new Pipeline()
      //.setStages(Array(labelIndexer, lrRegressor))
      .setStages(Array(lrRegressor))

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName(metricName)

    val paramGrid = new ParamGridBuilder()
      .addGrid(lrRegressor.maxIter, maxIter)
      .addGrid(lrRegressor.regParam, regParam)
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
        .foreach { case Row(individual: String, aBeta: String, label: Double, prob, prediction: Double) =>
          println(s"($individual, $aBeta)", prob, label==prediction)
        }
    }




    def modelFit(trainDF: DataFrame, testDF: DataFrame) = {
      // Build the cross-validated model
      val model: CrossValidatorModel = cv.fit(trainDF)

      // Pull out the best logistic regression model
      val lrModel: LogisticRegressionModel = model
        .bestModel
        .asInstanceOf[PipelineModel]
        .stages(1)
        .asInstanceOf[LogisticRegressionModel]

      // Get the size of the test DataFrame
      val total: Int = testDF.count.toInt

      // Make predictions on the test DataFrame
      val predictions: DataFrame = model.transform(testDF).select("individual", "aBeta", "label", "probability", "prediction")

      // Get RDD of predictions and labels
      val predictionsAndLabels = predictions.select("prediction", "label")
        .map(row => (row.getDouble(0), row.getDouble(1)))

      // Get metrics from above RDD
      val metrics = new MulticlassMetrics(predictionsAndLabels)

      // Print metrics/predictions/features/etc.
      printPredictions(predictions)
      println(metrics.confusionMatrix)
      println("Samples", total)
      println("Correct", (metrics.precision*total).toInt)
      println("Precision", metrics.precision)
      println("Recall", metrics.recall)
    }


    modelFit(trainingData, testData)








    //val model = new LogisticRegressionWithLBFGS()
    //  .setNumClasses(4)
    // .run(trainingData.map(_._4))


    //val labelsAndPredictions = testData.map { point =>
    //  val prediction = model.predict(point._4.features)
    //  (point._1, point._2, point._4.label, prediction)
    //}.map(p => (p._1, p._2, p._3, p._4))



    //println("Calculating metrics..")
    //val metrics = new MulticlassMetrics(labelsAndPredictions.map(p => (p._3, p._4)))
    //val precision = Array(metrics.precision(0), metrics.precision(1), metrics.precision(2))
    //val recall = Array(metrics.recall(0), metrics.recall(1), metrics.recall(2))



    //val metrics = new MulticlassMetrics(labelsAndPredictions.map(p => (p._3, p._4)))
    //val precision = Array(metrics.precision(0), metrics.precision(1), metrics.precision(2), metrics.precision(3))
    //val recall = Array(metrics.recall(0), metrics.recall(1), metrics.recall(2), metrics.recall(3))


    //println("GBR, ASW, CHB")
    //println(metrics.confusionMatrix.toString())

    //println("GBR - precision:" + precision(0) + " recall:" + recall(0))
    //println("ASW - precision:" + precision(1) + " recall:" + recall(1))
    //println("CHB - precision:" + precision(2) + " recall:" + recall(2))


    //val resultArray: Array[(Double, Double)] = labelsAndPredictions.map(p => (p._3, p._4)).collect
    //val clustered = "[%s]".format(resultArray.map(_._1.toString()).reduceLeft(_+","+_))
    //val expected = "[%s]".format(resultArray.map(_._2.toString()).reduceLeft(_+","+_))
    //val adjustedRandIndex = GetRandIndex(clustered, expected)
    //println("Adjusted Rand Index = " + adjustedRandIndex)


    //val errors = labelsAndPredictions.filter(r => r._3 != r._4).count.toDouble
    //val total = testData.count()
    //val testErr = errors / total

    //TestArray(a) = testErr
    //RandArray(a) = adjustedRandIndex.toDouble
    //val end = new Date().getTime
    //println("Job took "+(end-start)/1000 + " seconds")


    //println("Test errors: " + TestArray.mkString(", "))
    //println("Adjusted Rand Indices: " + RandArray.mkString(", "))
  }
  



  
  
  
}