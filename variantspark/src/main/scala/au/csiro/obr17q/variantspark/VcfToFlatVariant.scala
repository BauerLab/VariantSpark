package au.csiro.obr17q.variantspark

import org.apache.spark.rdd.RDD
import au.csiro.obr17q.variantspark.model.FlatVariant
import au.csiro.obr17q.variantspark.model.VcfParser

object VcfToFlatVariant extends SparkApp {
  conf.setAppName("VCF cluster")
  def main(args:Array[String]) {
    if (args.length < 1) {
        println("Usage: CsvClusterer <input-path> <output-path> <variant-cutoof>")
    }

    val VcfFiles = args(0)
    val output = args(1)
    val VariantCutoff = args(2).toInt
    val IndividualMeta: RDD[IndividualMap] = null
  
    //val PopFiles = Source.fromFile("data/PGPParticipantSurvey-20150831064509.csv").getLines()
    //val Populations = sc.parallelize(new PopulationMap(PopFiles, 1, ',', 0, 16 ).returnMap(IncludeGroups, ExcludeGroups))
    val vcfParser = new VcfParser(VcfFiles, VariantCutoff, IndividualMeta, sc)
    //val NoOfAlleles = vcfParser.variantCount
    val FilteredAlleles:RDD[FlatVariant] = vcfParser.individualVariants

    import sqlContext.implicits._

    FilteredAlleles.toDF().write.parquet(output)
  } 
}