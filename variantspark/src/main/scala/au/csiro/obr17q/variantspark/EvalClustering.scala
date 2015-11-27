package au.csiro.obr17q.variantspark

import au.csiro.obr17q.variantspark.CommonFunctions._
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import scala.io.Source
import au.csiro.pbdava.sparkle.LoanUtils
import com.github.tototoshi.csv.CSVReader
import au.csiro.obr17q.variantspark.metrics.Metrics

object EvalClustering extends SparkApp {
  conf.setAppName("VCF cluster")
  def main(args:Array[String]) {
    if (args.length < 1) {
        println("Usage: CsvClusterer <input-path>")
    }

    val inputFile = args(0)


    val PopFiles = Source.fromFile("data/ALL.panel").getLines()
    val Populations = sc.parallelize(new MetaDataParser(PopFiles, 1, '\t', "NA", 0, 1 ).returnMap(Array(), Array()))
    
    val SuperPopulationUniqueId = Populations.map(_.SuperPopulationId).distinct().zipWithIndex() //For ARI
    val superPopToId = SuperPopulationUniqueId.collectAsMap()
    val subjectToSuperPopId = Populations.map(ind => (ind.IndividualId, superPopToId(ind.SuperPopulationId))).collectAsMap()
    val zippedClustring = LoanUtils.withCloseable(CSVReader.open(inputFile)) { csvReader =>
      csvReader.iterator
      .filter(subjectAndCluster => subjectToSuperPopId.contains(subjectAndCluster(0)))
      .map(subjectAndCluster => (subjectToSuperPopId(subjectAndCluster(0)).toInt, subjectAndCluster(1).toInt)).toList
    }
    println(zippedClustring)
    val clustering = zippedClustring.unzip
    println("RandIndex:")
    println(Metrics.adjustedRandIndex(clustering._1, clustering._2))
  } 
}