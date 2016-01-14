package au.csiro.obr17q.variantspark

import au.csiro.obr17q.variantspark.CommonFunctions._
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector
import scala.io.Source
import au.csiro.obr17q.variantspark.algo.WideKMeans
import au.csiro.pbdava.sparkle.LoanUtils
import com.github.tototoshi.csv.CSVReader
import java.io.File
import java.io.FileReader
import com.github.tototoshi.csv.CSVWriter
import breeze.linalg.DenseVector
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.SparseVector
import breeze.linalg.DenseMatrix

object SparseWideAAT extends SparkApp {
  conf.setAppName("VCF cluster")
  
  def aggTTA(colNo:Int)(m:DenseMatrix[Int], v:Vector):DenseMatrix[Int] = {
    val sv = v.asInstanceOf[SparseVector]
    for (rowIdx <- 0 until sv.indices.length; colIdx <- 0 until sv.indices.length) { 
      val col = sv.indices(rowIdx)
      val row = sv.indices(colIdx)
      m(row,col)+=sv.values(rowIdx).toInt * sv.values(colIdx).toInt
    }
    m
  }
  
  def main(args:Array[String]) {

   
    if (args.length < 1) {
        println("Usage: CsvClusterer <input-path>")
    }

    val inputFiles = args(0)
    val output = args(1)
    
    val sparseVariat = sqlContext.read.parquet(inputFiles)
    println(sparseVariat.schema)

    
    val  data:RDD[Vector] = 
      sparseVariat.rdd
        .map(r=> Vectors.sparse(r.getInt(1),
          r.getSeq[Int](2).toArray, r.getSeq[Double](3).toArray))
    val test = data.cache().count()
    println(test)
  
    val colNo = data.first().size
    println(s"Col no: ${colNo}")
    // essentially we need to create a sparse vector   
    val result = data.aggregate(DenseMatrix.zeros[Int](colNo,colNo))(SparseWideAAT.aggTTA(colNo),  (v1,v2) => {v1+=v2})
    LoanUtils.withCloseable(CSVWriter.open(output)) { cswWriter =>
       for (row <- 0 until colNo ) {      
        cswWriter.writeRow(result(row,::).inner.toArray)
      }
    }
  } 
}