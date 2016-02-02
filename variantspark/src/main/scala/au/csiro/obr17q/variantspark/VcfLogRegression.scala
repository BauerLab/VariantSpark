package au.csiro.obr17q.variantspark

import java.util.Date

import au.csiro.obr17q.variantspark.CommonFunctions._
import au.csiro.obr17q.variantspark.model.ThousandGenomesVcfParser
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.mllib.regression.LabeledPoint

import scala.io.Source

object VcfLogRegression extends SparkApp {
  conf.setAppName("VCF logistic regression")
  
  def main(args:Array[String]) {
    val defaults = Array("data/data.vcf","0")
    val seed = 4363465
    val VcfFiles = if (args.length > 0) args(0) else defaults(0)
    val VariantCutoff = if (args.length > 1) args(1).toInt else defaults(1).toInt

    /**
     * TCGA settings
     * 00 - Individual
     * 01 - Population
     * 02 - Super Population
     * 03 - Gender
     */
    val PopFiles = Source.fromFile("data/ALL.panel").getLines()
    val IndividualMeta = sc.parallelize(new MetaDataParser(PopFiles, HeaderLines = 1, '\t', "", 0, 1 )(SexCol = 3).returnMap(IncludeGroups = Array("GBR","ASW", "CHB")))
    //val IndividualMeta = sc.parallelize(new MetaDataParser(PopFiles, HeaderLines = 1, '\t', "", 0, 2 )(SexCol = 3).returnMap())

    val vcfObject = new ThousandGenomesVcfParser(VcfFiles, VariantCutoff, IndividualMeta, sc, sqlContext)
    val NoOfAlleles = vcfObject.variantCount

    val FilteredAlleles = vcfObject.individualTuples

    val data = vcfObject.data
    /*
    val IndividualVariants = FilteredAlleles
    .groupByKey
    //.map(p => (p._1.split('_')(0).substring(0,12), (p._1.split('_')(1), p._2))) // TCGA data
    .map(p => (p._1, (p._1, p._2)))
    .join(IndividualMeta.map(_.toPops)) //filter out individuals lacking required data 
    /*
    .map(h =>
      (h._1, h._2._1._1, h._2._2, LabeledPoint(if (h._2._2 =="EUR") 0
                  else if (h._2._2 =="AFR") 1 else if (h._2._2 =="AMR") 2
                  else if (h._2._2 =="EAS") 3
                  else -1, Vectors.sparse(NoOfAlleles, h._2._1._2.to[Seq] )) // Binary labels
      ))
      */
    .map(h =>
      (h._1, h._2._1._1, h._2._2, LabeledPoint(if (h._2._2 =="GBR") 0
                  else if (h._2._2 =="ASW") 1 else if (h._2._2 =="CHB") 2
                  else -1, Vectors.sparse(NoOfAlleles, h._2._1._2.to[Seq] )) // Binary labels
      )) 


    //.filter(_._2 == "NORMAL")
    .cache
     */




    println(data.count() + " individuals")
    println("with " + NoOfAlleles + " alleles")



    val TestArray: Array[Double] = new Array(10)
    val RandArray: Array[Double] = new Array(10)


    val Array(trainingData, testData) = data.randomSplit(Array(0.8, 0.2))



    val lr = new LogisticRegression()


    lr.setMaxIter(10)
      .setRegParam(0.01)

    val model1 = lr.fit(trainingData)


    model1.transform(testData)



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