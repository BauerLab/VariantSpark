package au.csiro.obr17q.variantspark

import au.com.bytecode.opencsv.CSVParser
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.{Vector=>MLVector, Vectors}
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.regression.LabeledPoint


/**
 * @author obr17q
 */
object VcfForest extends SparkApp {
  conf.setAppName("VCF foresting")
    
  def main(args:Array[String]) {
    val numClasses = 2 // 0 - healthy, 1 - obese
    val categoricalFeaturesInfo = Map[Int, Int]((0, 2))
    val numTrees = 5
    val featureSubsetStrategy = "auto"
    val impurity = "gini"
    val maxDepth = 6
    val maxBins = 100
    val seed = 4363465
  

    val genoFiles = sc.textFile("data/tcga_csv.csv");

    // Population information
    val Populations = genoFiles
    .filter( !_.startsWith("bcr_sample_barcode") )
    .mapPartitions ( lines => {
      val parser = new CSVParser(',')
      lines.map ( line => {
        val l = parser.parseLine(line)
        val name = l(0)
        val obese = l(1).toDouble
        val genotype = l.slice(2,l.length).map(_.toDouble)
        val genotypeVector = Vectors.dense(genotype)
        val n = l.length
        LabeledPoint(obese, genotypeVector)
        //( name, obese, v ) // IndividualID, ObesityStatus, Relationship
      } )
    } )


    val splits = Populations.randomSplit(Array(0.6, 0.4))
    val (trainingData, testData) = (splits(0), splits(1))     
    //trainingData.foreach(x => println(x.label))

    val model = RandomForest.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
        numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)

          
    // Evaluate model on test instances and compute test error
    val labelAndPreds = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / testData.count()
    println("Learned classification forest model:\n" + model.totalNumNodes)
    println("Test Error = " + testErr)
    labelAndPreds.foreach(println)
    
  }
}