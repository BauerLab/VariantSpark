package au.csiro.obr17q.variantspark.model

import scala.math._
import scala.util.Random
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._

/**
  * Created by obr17q on 5/01/2016.
  */

private case class FakeDataRecord(individual: String, simulationOne: Double, simulationTwo: Double, features: Vector)

class FakeDataParser (val samples: Int, val dims: Int, val save:Boolean = false, val sc: SparkContext, val sqlContext: SQLContext) extends scala.Serializable {




  /**
    * RDD of an line Arrays
    */
  private def CsvLineRDD: RDD[(Array[Double], Long)] = {
    val dims = this.dims
    sc.parallelize( List.fill(samples)(Array))
      .map(q => q.fill(dims)(Random.nextDouble))
      .zipWithIndex
  }

  /**
    * RDD of Variant IDs zipped with a unique index.
    * RDD[(Variant, VariantIndex)]
    * RDD[(PGM1, 1221)]
    * count = noOfGenes
    */
  def featureTuples: List[(String, Int)] = {
    (1 to dims).toList
      .map(name => s"col$name")
      .zipWithIndex
  }


  val data = sqlContext
    .createDataFrame {
      val featureTuples = this.featureTuples
      val dims = this.dims
      val r = Random.nextDouble
      CsvLineRDD
        .map{ case(x, id) =>
          val featureVector = x.zip(featureTuples).map(q => (q._2._2, q._1.toDouble))
          FakeDataRecord(
            individual = s"row$id",
            simulationOne = 10 * sin(10 * Pi * x(0)),// + r,
            simulationTwo = 10 * sin(Pi * x(0) * x(1)) + 20 * pow(x(2) - 0.05, 2) + 10 * x(3) + 5 * x(4),
            features = Vectors.sparse(dims, featureVector )
          )
        }
  }.toDF

  def feature(n: String) : Int = {
    featureTuples.filter(_._1 == n).head._2
  }

  def merge(srcPath: String, dstPath: String): Unit =  {
    val hadoopConfig = new Configuration()
    val hdfs = FileSystem.get(hadoopConfig)
    FileUtil.copyMerge(hdfs, new Path(srcPath), hdfs, new Path(dstPath), false, hadoopConfig, null)
  }

  def saveAs(path: String) = {

    println(s"Writing file to $path/part-00000")
    val header: RDD[String] = sc.parallelize(Array("\"\",\"aBeta\"," + featureTuples.map(_._1).mkString(",")))
    data.map(row =>
      s"${row.getString(0)},${row.getDouble(2).toInt}," +
      row.getAs[Vector]("features").toArray
        .mkString(","))
        .union(header)
        .coalesce(1, shuffle=true).saveAsTextFile(path)

    println("Done.")
  }
}
