package au.csiro.obr17q.variantspark

import au.com.bytecode.opencsv.CSVParser
import au.csiro.obr17q.variantspark.CommonFunctions.variantDist
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

/**
 * @author obr17q
 */
class VcfParser (val VcfFileNames: String, val VariantCutoff: Int, val sc: SparkContext) extends java.io.Serializable {


  private val NotAVariant = "#CHROM" :: "POS" :: "REF" :: "ALT" :: "QUAL" :: "FILTER" :: "INFO" :: "FORMAT" :: Nil

  private val VcfFilesRDD = sc.textFile(VcfFileNames, 20)


  /**
    * Returns the heading row from the FASTA file
    * The heading from each column is stored as a String in a List.
    * List[String]
    */
  private def getHeadings(VcfFiles: RDD[String]) : List[String] = {
    VcfFiles
      .filter( _.startsWith("#CHROM") )
      .map(
        line => {
          val parser = new CSVParser('\t')
          parser.parseLine(line)
        } ).map(e => List(e:_*)).first()
  }


  /**
   * Array of elements for each line in the VCF file
   * Array elements are zipped with the VCF heading
   * AllVariants: RDD[(VariantID, Array[(IndividualID, Variant)])]
   */
  private val VcfLineRdd : RDD[(Long, Array[(String, Double)])] = {
    val VariantCutoff = this.VariantCutoff
    val NotAVariant = this.NotAVariant
    val Headings = getHeadings(VcfFilesRDD)
    VcfFilesRDD
    .filter( !_.startsWith("#") )
    .mapPartitions( lines => {
      val parser = new CSVParser('\t')
      lines.map( line => {
        parser.parseLine(line)
        .zip(Headings)
      } )
    } )
    .map( line => (
        "%s-%s".format(line(0)._1, line(1)._1 ),
        line.filter(v => !(NotAVariant contains v._2)) // Filter out the metadata tuples
        .map(v => (v._2, variantDist(v._1, 0))) // Convert variant String to Double
        .filter(v => v._2 != 0) //Filter out zero variants to make dataset sparse
        /** Add aditional filters here to apply to each variant. **/
    ))
    .filter(h => h._2.length > VariantCutoff) // Filter out 'rare' variants
    /** Add additional filters here to apply to all variants at each allele. **/
    .zipWithIndex()
    .map(h => (h._2, h._1._2))
  }


  /**
    * Number of variants in the file
    */
  val variantCount : Int =
    VcfLineRdd.count().toInt


  def individualTuples : RDD[(String, (Int, Double))] = {
    VcfLineRdd
    .flatMap( (h) => {
      h._2
      .map( i => ( i._1, (h._1.toInt, i._2) ) )
    })
  }
  
}