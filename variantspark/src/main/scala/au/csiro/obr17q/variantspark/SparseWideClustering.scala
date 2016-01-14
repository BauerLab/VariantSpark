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

object SparseWideClustering extends SparkApp {
  conf.setAppName("VCF cluster")
  def main(args:Array[String]) {

   
    if (args.length < 1) {
        println("Usage: CsvClusterer <input-path>")
    }

    val inputFiles = args(0)
    val k = args(1).toInt
    val iter = args(2).toInt
    val output = args(3)
    
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    val sparseVariat = sqlContext.parquetFile(inputFiles)    
    println(sparseVariat.schema)

    
    val data = 
      sparseVariat.rdd
        .map(r=> Vectors.sparse(r.getInt(1),
          r.getSeq[Int](2).toArray, r.getSeq[Double](3).toArray))
    val test = data.cache().count()
    println(test)
    
    val kmeans = new WideKMeans(k, iter)
    val result  = kmeans.run(data)  
    println("Result:")
    val clusterAssignment = kmeans.assignClusters(data, result).toList
    println(clusterAssignment)
    
    val indexSubjectMap = LoanUtils.withCloseable(CSVReader.open(new FileReader(new File(inputFiles, "_index.csv")))){
      csvReader =>
        csvReader.iterator.map { x => (x(1).toInt,x(0))}.toMap   
    }
    
    LoanUtils.withCloseable(CSVWriter.open(output)) { cswWriter =>
      clusterAssignment.zipWithIndex.map{ case (cluster,index) => (indexSubjectMap(index), cluster)}
        .foreach(t => cswWriter.writeRow(t.productIterator.toSeq))
    }
  
  } 
}