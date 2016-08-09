package au.csiro.obr17q.variantspark.model

import au.com.bytecode.opencsv.CSVParser
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

/**
  * Created by obr17q on 5/01/2016.
  */

private case class CsvRecord(individual: String, sampleType: String, bmi: Double, msi_status: String, bmi_cat: String, features: Vector)

class CsvParser (val CsvFileNames: String, val VariantCutoff: Int, val sc: SparkContext, val sqlContext: SQLContext) extends scala.Serializable {

  private val CsvFilesRDD = sc.textFile(CsvFileNames, 5)

  /**
    * RDD of an line Arrays
    */
  private def CsvLineRDD: RDD[Array[String]] = {
    CsvFilesRDD
      .filter(!_.startsWith("\"dbSNP_RS"))
      .mapPartitions(lines => {
        val parser = new CSVParser
        lines.map(line => {
          parser.parseLine(line)
        })
      })
  }.cache

  /**
    * RDD of Variant IDs zipped with a unique index.
    * RDD[(Variant, VariantIndex)]
    * RDD[(PGM1, 1221)]
    * count = noOfGenes
    */
  def featureTuples: List[(String, Int)] = {
    CsvLineRDD
      .map(line => line(7))
      .distinct
      .zipWithIndex
      .map(p => (p._1, p._2.toInt)).collect.toList
  }

  /**
    * RDD[(IndividualID, (BMI, BMI_CAT, MSI_STATUS))]
    * RDD[(TCGA-CA-6717, (30.25, "obese", 2))]
    * count = noOfIndividuals
    */
  private def IndividualMetaData: RDD[(String, (Double, String, String))] = {
    CsvLineRDD
      .map(line => (line(10), (line(27).toDouble, line(28), line(23))))
      .distinct
  }

  val data = sqlContext
    .createDataFrame {
      val variantCount = this.variantCount
      val featureTuples = this.featureTuples
      val IndividualMetaData = this.IndividualMetaData
      CsvLineRDD
        .map(line => ((line(10), line(7)), 1))
        .reduceByKey(_ + _)
        .map(p => (p._1._2, (p._1._1, p._2.toDouble)))
        .join(sc.parallelize(featureTuples))
        .map(p => (p._2._1._1, (p._2._2, p._2._1._2)))
        .groupByKey
        .join(IndividualMetaData)
        .filter(_._2._2._3 == "MSS")
        .map(p =>
          CsvRecord(
            individual = p._1,
            sampleType = p._1,
            bmi = p._2._2._1,
            msi_status = p._2._2._3,
            bmi_cat = p._2._2._2,
            features = Vectors.sparse(variantCount, p._2._1.to[Seq])
          )
        )
    }.toDF


  /**
    * Number of variants in the file
    */
  private def variantCount : Int = {
    featureTuples.size
  }
}
