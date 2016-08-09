package au.csiro.obr17q.variantspark.model

import au.com.bytecode.opencsv.CSVParser
import au.csiro.obr17q.variantspark.CommonFunctions.variantDist
import au.csiro.obr17q.variantspark.IndividualMap
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

/**
 * @author obr17q
 */

case class VcfRecord(
                      individual: String,
                      sampleType: String = null,
                      bmi: Double = 0.0,
                      population: String = null,
                      superPopulation: String = null,
                      features: Vector
                    )

class VcfParser (val VcfFileNames: String, val VariantCutoff: Int, val sc: SparkContext, val sqlContext: SQLContext)(val IndividualMeta: RDD[IndividualMap]) extends scala.Serializable {

  val NotAVariant = "#CHROM" :: "POS" :: "ID" :: "REF" :: "ALT" :: "QUAL" :: "FILTER" :: "INFO" :: "FORMAT" :: Nil

  val VcfFilesRDD = sc.textFile(VcfFileNames, 20)


  /**
    * Returns the heading row from the FASTA file
    * The heading from each column is stored as a String in a List.
    * List[String]
    */
  def getHeadings(VcfFiles: RDD[String]) : List[String] = {
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
   * AllVariants: RDD[(VariantID, chr:pos, Array[(IndividualID, Variant)])]
   */
  private val VcfLineRdd : RDD[(Long, String, Array[(String, Double)])] = {
    val VariantCutoff = this.VariantCutoff
    val NotAVariant = this.NotAVariant
    val Headings = getHeadings(VcfFilesRDD)
    println(s"Individuals: ${Headings.length - 9}")

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


  def data = sqlContext
    .createDataFrame {
      val variantCount = this.variantCount
      println(s"Variants: $variantCount")
      individualTuples
        .groupByKey //group by individual ID, i.e. get RDD of individuals
        //.map(p => (p._1.split('_')(0).substring(0, 12), (p._1.split('_')(1), p._2))) // Split the TCGA key to get ID & type
        //.filter(_._2._1 == "NORMAL")
        //.map(p => (p._1, (p._1, p._2))) // 1000 data
        //.join(IndividualMeta.map(_.toBMI)) //filter out individuals lacking required data
        .map(h =>
        VcfRecord(
          individual = h._1,
          features = Vectors.sparse(variantCount, h._2.to[Seq]))
        )
    }


  /**
    * Number of variants in the file
    */
  def variantCount : Int = {
    VcfLineRdd.count.toInt
  }

  /**
    * Returns tuples of alleles with their uniqueIDs
    * RDD[(chr-location, alleleID)]
    *
    * @return
    */
  def alleleTuples : RDD[(String, Int)] = {
    VcfLineRdd
    .map(h => (h._2, h._1.toInt))
  }


  def individualTuples : RDD[(String, (Int, Double))] = {
    VcfLineRdd
      .flatMap( (h) => {
        h._3
          .map( i => ( i._1, (h._1.toInt, i._2) ) )
        })
    }


  /**
    * @return
    * RDD[ FlatVariant(subjectId:String, variantIndex:Int, allele:Double)]
    */

  def individualVariants : RDD[FlatVariant] = {
    VcfLineRdd
    .flatMap( h => {
      h._3
      .map( i => FlatVariant( i._1, h._1.toInt, i._2))
    })
  }
}
