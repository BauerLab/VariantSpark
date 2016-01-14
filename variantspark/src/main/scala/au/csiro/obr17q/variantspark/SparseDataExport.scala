package au.csiro.obr17q.variantspark

import au.csiro.obr17q.variantspark.CommonFunctions._
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import scala.io.Source
import au.csiro.obr17q.variantspark.algo.WideKMeans
import au.csiro.pbdava.sparkle.LoanUtils
import com.github.tototoshi.csv.CSVReader
import java.io.File
import java.io.FileReader
import com.github.tototoshi.csv.CSVWriter
import au.csiro.obr17q.variantspark.algo.WideDecisionTree
import au.csiro.obr17q.variantspark.algo.WideRandomForest

object SparseDataExport extends SparkApp {
  conf.setAppName("VCF cluster")
  def main(args:Array[String]) {

   
    if (args.length < 1) {
        println("Usage: CsvClusterer <input-path>")
    }

    val inputFiles = args(0)
    val output = args(1)
    
    val sparseVariat = sqlContext.read.parquet(inputFiles)
    println(sparseVariat.schema)

   val indexSubjectMap = LoanUtils.withCloseable(CSVReader.open(new FileReader(new File(inputFiles, "_index.csv")))){
      csvReader =>
        csvReader.iterator.map { x => (x(1).toInt,x(0))}.toMap   
    }    
    
    val PopFiles = Source.fromFile("data/ALL.panel").getLines()
    val Populations = sc.parallelize(new MetaDataParser(PopFiles, 1, '\t', "NA", 0, 1 ).returnMap(Array(), Array()))
    
    val SuperPopulationUniqueId = Populations.map(_.SuperPopulationId).distinct().zipWithIndex() //For ARI
    val superPopToId = SuperPopulationUniqueId.collectAsMap()
    val subjectToSuperPopId= Populations.map(ind => (ind.IndividualId, superPopToId(ind.SuperPopulationId))).collectAsMap()

    val allLabels = indexSubjectMap.toStream.sorted
      .map({case (index,subject) => 
        subjectToSuperPopId.getOrElse(subject, superPopToId.size.toLong).toInt
      }).toArray
    
   
   val unknownLabel = superPopToId.size
   val unknownLabelsCount = allLabels.count( _ == unknownLabel)
   println(s"Unknown labels count: ${unknownLabelsCount}")
   // need to get of unknow data   
      
   val indexCorrections = Array.fill(allLabels.length)(0)
  
   // I am sure there is a way to do this in a nice functional programming way but for now
   var counts = 0;
   allLabels.zipWithIndex.foreach { case(v,i) => 
     if (v == unknownLabel) {
       counts += 1
     }
     indexCorrections(i) = counts
   }
   
   
   val data = 
      sparseVariat.rdd      
        .map{r=> 
          val (size, indexes,values) = (r.getInt(1),r.getSeq[Int](2).toArray, r.getSeq[Double](3))
          Vectors.sparse(size - unknownLabelsCount,
          indexes.filter(i => allLabels(i) < unknownLabel).map(i => i - indexCorrections(i)).toArray,
          values.zipWithIndex.filter({ case(v,i) => allLabels(indexes(i)) < unknownLabel }).map(_._1).toArray)
         }
    val test = data.cache().count()
    println(test)    
    
    val labels = allLabels.filter(_ < unknownLabel)

      
      
      LoanUtils.withCloseable(CSVWriter.open(output)) { cswWriter =>
      cswWriter.writeRow(labels.toSeq)
      data.collect().foreach { v => cswWriter.writeRow(v.toArray.map(_.toInt)) }
    }
  } 
}