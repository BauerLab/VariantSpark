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

object SparseWideForest extends SparkApp {
  conf.setAppName("VCF cluster")
  def main(args:Array[String]) {

   
    if (args.length < 1) {
        println("Usage: CsvClusterer <input-path>")
    }

    val inputFiles = args(0)
    val output = args(1)
    
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    val sparseVariat = sqlContext.parquetFile(inputFiles)    
    println(sparseVariat.schema)

   val indexSubjectMap = LoanUtils.withCloseable(CSVReader.open(new FileReader(new File(inputFiles, "_index.csv")))){
      csvReader =>
        csvReader.iterator.map { x => (x(1).toInt,x(0))}.toMap   
    }    
    val data = 
      sparseVariat.rdd
        .map(r=> Vectors.sparse(r.getInt(1),
          r.getSeq[Int](2).toArray, r.getSeq[Double](3).toArray))
    val test = data.cache().count()
    println(test)
    
    val PopFiles = Source.fromFile("data/ALL.panel").getLines()
    val Populations = sc.parallelize(new MetaDataParser(PopFiles, 1, '\t', "NA", 0, 1 ).returnMap(Array(), Array()))
    
    val SuperPopulationUniqueId = Populations.map(_.SuperPopulationId).distinct().zipWithIndex() //For ARI
    val superPopToId = SuperPopulationUniqueId.collectAsMap()
    val subjectToSuperPopId= Populations.map(ind => (ind.IndividualId, superPopToId(ind.SuperPopulationId))).collectAsMap()

    val labels = indexSubjectMap.toStream.sorted
      .map({case (index,subject) => 
        subjectToSuperPopId.getOrElse(subject, superPopToId.size.toLong).toInt
      }).toArray
     
    
    
    val rf = new WideRandomForest()
    val result  = rf.run(data,labels.toArray, 100)
    //println(result)
    //result.printout()
    val variableImportnace = result.variableImportance
    
    variableImportnace.toSeq.sortBy(-_._2).take(20).foreach(println)
    
    
    //LoanUtils.withCloseable(CSVWriter.open(output)) { cswWriter =>
    //  clusterAssignment.zipWithIndex.map{ case (cluster,index) => (indexSubjectMap(index), cluster)}
    //    .foreach(t => cswWriter.writeRow(t.productIterator.toSeq))
    //}
  
  } 
}