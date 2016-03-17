package au.csiro.obr17q.variantspark.model

import scala.math._
import scala.util.Random
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

/**
  * Created by obr17q on 5/01/2016.
  */

private case class FakeDataRecord(individual: String, simulationOne: Double, simulationTwo: Double, features: Vector)

class FakeDataParser (val n: Int, val p: Int, val sc: SparkContext, val sqlContext: SQLContext) extends scala.Serializable {




  /**
    * RDD of an line Arrays
    */
  private def CsvLineRDD: RDD[(Array[Double], Long)] = {
    val p = this.p
    sc.parallelize( List.fill(n)(Array))
      .map(q => q.fill(p)(Random.nextDouble))
      .zipWithIndex
  }

  /**
    * RDD of Variant IDs zipped with a unique index.
    * RDD[(Variant, VariantIndex)]
    * RDD[(PGM1, 1221)]
    * count = noOfGenes
    */
  def featureTuples: List[(String, Int)] = {
    (1 to p).toList
      .map(name => s"col$name")
      .zipWithIndex
  }


  val data = sqlContext
    .createDataFrame {
      val featureTuples = this.featureTuples
      val p = this.p
      val r = Random.nextDouble
      CsvLineRDD
        .map{ case(x, id) =>
          val featureVector = x.zip(featureTuples).map(q => (q._2._2, q._1.toDouble))
          FakeDataRecord(
            individual = s"row$id",
            simulationOne = 10 * sin(10 * Pi * x(0)),// + r,
            simulationTwo = 10 * sin(Pi * x(0) * x(1)) + 20 * pow(x(2) - 0.05, 2) + 10 * x(3) + 5 * x(4),
            features = Vectors.sparse(p, featureVector )
          )
        }
  }.toDF

  def feature(n: String) : Int = {
    featureTuples.filter(_._1 == n).head._2
  }
}
