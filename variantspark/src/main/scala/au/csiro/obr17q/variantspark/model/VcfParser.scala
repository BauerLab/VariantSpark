package au.csiro.obr17q.variantspark.model

import au.com.bytecode.opencsv.CSVParser
import au.csiro.obr17q.variantspark.CommonFunctions.variantDist
import au.csiro.obr17q.variantspark.IndividualMap
import au.csiro.obr17q.variantspark.VcfForest._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg._
import model.FlatVariant

/**
 * @author obr17q
 */

private case class VcfRecord(individual: String, sampleType: String, bmi: Double, preLabel: String, features: Vector)

class VcfParser (val VcfFileNames: String, val VariantCutoff: Int, val IndividualMeta: RDD[IndividualMap], val sc: SparkContext) extends scala.Serializable {

  private val NotAVariant = "#CHROM" :: "POS" :: "REF" :: "ALT" :: "QUAL" :: "FILTER" :: "INFO" :: "FORMAT" :: Nil

  private val VcfFilesRDD = sc.textFile(VcfFileNames, 20)


  /**
    * Returns the heading row from the FASTA file
    * The heading from each column is stored as a String in a List.
    * List[String]
    */
  private def getHeadings(VcfFiles: RDD[String]) : List[String] = {
    VcfFiles
      .filter(_.startsWith("#CHROM"))
      .map(
        line => {
          val parser = new CSVParser('\t')
          parser.parseLine(line)
        } ).map(e => List(e:_*)).first
  }


  /**
   * Array of elements for each line in the VCF file
   * Array elements are zipped with the VCF heading
   * AllVariants: RDD[(VariantID, chr-pos, Array[(IndividualID, Variant)])]
   */
  private val VcfLineRdd : RDD[(Long, String, Array[(String, Double)])] = {
    val VariantCutoff = this.VariantCutoff
    val NotAVariant = this.NotAVariant
    val Headings = getHeadings(VcfFilesRDD)
    VcfFilesRDD
    .filter(!_.startsWith("#"))
    .mapPartitions( lines => {
      val parser = new CSVParser('\t')
      lines.map( line => {
        parser.parseLine(line)
        .zip(Headings)
      } )
    } )
    .map( line => (
        "%s:%s".format(line(0)._1, line(1)._1 ),
        line.filter(v => !(NotAVariant contains v._2)) // Filter out the metadata tuples
        .map(v => (v._2, variantDist(v._1, 0))) // Convert variant String to Double
        .filter(v => v._2 != 0) //Filter out zero variants to make dataset sparse
        /** Add aditional filters here to apply to each variant. **/
    ))
    .filter(h => h._2.length > VariantCutoff) // Filter out 'rare' variants
    /** Add additional filters here to apply to all variants at each allele. **/
    .zipWithIndex
    .map(h => (h._2, h._1._1,  h._1._2))
  }


  val data = sqlContext
    .createDataFrame {
      individualTuples
        .groupByKey //group by individual ID, i.e. get RDD of individuals
        .map(p => (p._1.split('_')(0).substring(0, 12), (p._1.split('_')(1), p._2))) // Split the TCGA key to get ID & type
        .filter(_._2._1 == "NORMAL")
        //.map(p => (p._1, (p._1, p._2))) // 1000 data
        .join(IndividualMeta.map(_.toBMI)) //filter out individuals lacking required data
        .map(h =>
        VcfRecord(
            individual = h._1,
            sampleType = h._2._1._1,
            bmi = h._2._2,
            preLabel = if (h._2._2 > 40) "obese" else if (h._2._2 > 30) "overweight" else "healthy",
            features = Vectors.sparse(variantCount, h._2._1._2.to[Seq]))
      )
    }



  /**
    * Number of variants in the file
    */
  def variantCount : Int =
    VcfLineRdd.count.toInt


  def alleleTuples : RDD[(String, Int)] = {
    VcfLineRdd
    .map(h => (h._2, h._1.toInt))
  }

/*
  def individualTuples : RDD[(String, (Int, Double))] = {
    VcfLineRdd
      .map(h => (h._1, h._3))
      .flatMap( (h) => {
        h._2
          .map( i => ( i._1, (h._1.toInt, i._2) ) )
        })
    }
*/
  
  def individualVariants : RDD[FlatVariant] = {
    VcfLineRdd
    .flatMap( (h) => {
      h._2
      .map( i => FlatVariant( i._1, h._1.toInt, i._2))
    })
  }
}
