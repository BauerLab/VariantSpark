package au.csiro.obr17q.variantspark.model

import au.com.bytecode.opencsv.CSVParser
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

/**
  * Created by obr17q on 5/01/2016.
  */

private case class GenericRecord(individual: String, y: Double, features: Vector)

class GenericParser (val CsvFileNames: String, val sc: SparkContext, val sqlContext: SQLContext) extends scala.Serializable {

  val NotAVariant = "" :: "aBeta" :: Nil

  private val CsvFilesRDD = sc.textFile(CsvFileNames, 5)


  /**
    * Returns the heading row from the FASTA file
    * The heading from each column is stored as a String in a List.
    * List[String]
    */
  val headings: List[String] = {
    CsvFilesRDD
      .filter(_.startsWith(""""y","""))
      .map(
        line => {
          val parser = new CSVParser
          parser.parseLine(line)
        } ).map(e => List(e:_*)).first.drop(1)
  }



  /**
    * @return
    * RDD[ Array(column1:String, column2:String, ..., columnN:String)]
    */
  private def CsvLineRDD: RDD[Array[String]] = {
    CsvFilesRDD
      .filter(!_.startsWith(""""y","""))
      .mapPartitions(lines => {
        val parser = new CSVParser
        lines.map(line => {
          parser.parseLine(line)
        })
      })
  }.cache

  /**
    * RDD of Variant IDs zipped with a unique index.
    * @return
    * RDD[(Variant:String, VariantIndex:Int)]
    * RDD[(PGM1, 1221)]
    */
  def featureTuples: List[(String, Int)] = {
    //val NotAVariant = this.NotAVariant
    headings
      .filter(v => !(NotAVariant contains v))
      .zipWithIndex
  }

  /**
    * RDD[(IndividualID:String, abStatus:Int))]
    * count = noOfIndividuals
    */
  def IndividualMetaData: RDD[(String, Int)] = {
    CsvLineRDD
      .map(line => (line(0), line(1).toDouble.toInt))
      .distinct
  }

  /**
    * @return
    * RDD[ FlatVariant(subjectId:String, variantIndex:Int, allele:Double)]
    */

  def individualVariants: RDD[FlatVariant] = {
    val featureTuples = this.featureTuples
    CsvLineRDD
      .map(p => (p(0), p.drop(2).zip(featureTuples)))
      .flatMap(p => p._2.map(q => FlatVariant(p._1, q._2._2, q._1.toDouble)))
  }



  val data = sqlContext
    .createDataFrame {
      val featureCount = this.featureCount
      val featureTuples = this.featureTuples
      CsvLineRDD
        .map(p => (p(0), p(1), p.drop(2).zip(featureTuples).map(q => (q._2._2, q._1.toDouble))))
        .map(p =>
          GenericRecord(
            individual = p._1,
            y = p._2.toDouble,
            features = Vectors.sparse(featureCount, p._3)
          )
        )
  }.toDF

  /**
    * Number of variants in the file
    */
  private def featureCount : Int = {
    headings.length
  }

  def feature(n: String) : Int = {
    featureTuples.filter(_._1 == n).head._2
  }
}
