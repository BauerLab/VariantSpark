package au.csiro.obr17q.variantspark

import au.csiro.obr17q.variantspark.CommonFunctions._
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark
import scala.io.Source
import org.apache.spark.sql.functions.max
import org.apache.spark.rdd.RDD
import au.csiro.obr17q.variantspark.model.SubjectSparseVariant
import it.unimi.dsi.fastutil.ints.Int2DoubleOpenHashMap
import it.unimi.dsi.fastutil.ints.Int2DoubleMap
import it.unimi.dsi.fastutil.ints.Int2DoubleRBTreeMap
import org.apache.spark.mllib.linalg.SparseVector
import scala.collection.mutable.HashMap
import au.csiro.obr17q.variantspark.model.LocusSparseVariant
import au.csiro.pbdava.sparkle.LoanUtils
import com.github.tototoshi.csv.CSVWriter
import java.io.FileWriter
import java.io.File


object FlatVariantToSparseByPosition extends SparkApp {
  
  def map2Sparse(size:Int, map: Int2DoubleMap):SparseVector = {
     val sorted = new Int2DoubleRBTreeMap(map);
     Vectors.sparse(size, sorted.keySet().toIntArray(), sorted.values().toDoubleArray()).toSparse
  }
  
  conf.setAppName("VCF cluster")
  def main(args:Array[String]) {

    if (args.length < 1) {
        println("Usage: CsvClusterer <input-path>")
    }

    val inputFiles = args(0)
    val output = args(1) 
    val chunks = args(2).toInt
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    val flatVariandDF = sqlContext.parquetFile(inputFiles)    
    println(flatVariandDF.schema)
    val subjectIds = flatVariandDF.select("subjectId").distinct().collect()
 
    // now 
    
    val subjectToIndexMap = subjectIds.map(_.getString(0)).zipWithIndex.toMap
    val subjectsSize = subjectToIndexMap.size
    val br_subjectToIndexMap = sc.broadcast(subjectToIndexMap)
    println(subjectToIndexMap)
    
    val parirRDD:RDD[(Int,(Int,Double))] = flatVariandDF.rdd.map(r => (r.getInt(1), (br_subjectToIndexMap.value(r.getString(0)), r.getDouble(2))))
    parirRDD
      .groupByKey(chunks)
      .mapValues(i => Vectors.sparse(subjectsSize,i.to[Seq]).toSparse)
      .map {case (locusId, sparseVector) =>
        LocusSparseVariant(locusId, sparseVector.size, sparseVector.indices, sparseVector.values)}
      .toDF().saveAsParquetFile(output)  
      
    // and also commit the dictionary
    LoanUtils.withCloseable(new CSVWriter(new FileWriter(new File(output, "_index.csv")))) { cvsWriter =>
        subjectToIndexMap.foreach(t => cvsWriter.writeRow(t.productIterator.toSeq))
    }
  } 
}