package au.csiro.obr17q.variantspark

import org.apache.spark.mllib.feature.ChiSqSelector
import org.apache.spark.mllib.linalg.{Vector => MLVector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint

import scala.io.Source


/**
 * @author obr17q
 */
object VcfSelection extends SparkApp {
  conf.setAppName("VCF Feature Selection")
    
  def main(args:Array[String]) {

    

    val VcfFiles = args(0)
    val Features = args(1).toInt
    val VariantCutoff = args(4).toInt

    
    // (..., "", 0, 2) for super-populations, (..., "", 0, 1) for populations Remember to change filters etc.
    val PopFiles = Source.fromFile("data/ALL.panel").getLines()
    //val IndividualMeta = sc.parallelize(new MetaDataParser(PopFiles, HeaderLines = 1, '\t', "", 0, 2)(SexCol = 3, extra1 = 1).returnMap()) //Super-populations
    val IndividualMeta = sc.parallelize(new MetaDataParser(PopFiles, HeaderLines = 1, '\t', "", 0, 1)(SexCol = 3, extra1 = 2).returnMap())

    
    val vcfObject = new VcfParser(VcfFiles, VariantCutoff, sc)

    val NoOfAlleles = vcfObject.variantCount

    val FilteredAlleles = vcfObject.individualTuples
    
    val IndividualVariants = FilteredAlleles
    .groupByKey //group by individual ID, i.e. get RDD of individuals
    //.map(p => (p._1.split('_')(0).substring(0,12), (p._1.split('_')(1), p._2)))
    .map(p => (p._1, (p._1, p._2)))
    .join(IndividualMeta.map(_.toIndo)) //filter out individuals lacking required data 
    .map(h =>
      
      /*(h._1, h._2._2._1, h._2._2._2, LabeledPoint(
                  if (h._2._2._1 =="EUR") 0
                  else if (h._2._2._1 =="AFR") 1 else if (h._2._2._1 =="AMR") 2
                  else if (h._2._2._1 =="EAS") 3
                  else -1, Vectors.sparse(NoOfAlleles, h._2._1._2.to[Seq] )
                  ))*/
      
      (h._1, h._2._2._1, h._2._2._2, LabeledPoint(
                  if (h._2._2._1 =="GBR") 0
                  else if (h._2._2._1 =="ASW") 1 else if (h._2._2._1 =="CHB") 2
                  else -1, Vectors.sparse(NoOfAlleles, h._2._1._2.to[Seq] )
                  ))

    )
    //.filter(_._2 == "NORMAL")
    .cache
    //def isObese(i: Double) = i >= 30
    //val obese = SparseVariants.filter(h => isObese(h._3)).count
    //val healthy = SparseVariants.count - obese
    //println("Totals:")
    //println(obese + " obese")
    //println(healthy + " healthy")
    println("Selecting " + Features + " features from " + NoOfAlleles + " alleles")    
    
    
    
    val labeled = IndividualVariants.map(_._4).cache
    val selector = new ChiSqSelector(Features)
    val transformer = selector.fit(labeled)
    val filteredData = IndividualVariants.map { lp =>
      (lp._1, lp._2, lp._3, LabeledPoint(lp._4.label, transformer.transform(lp._4.features)))
    }
    
    filteredData.saveAsObjectFile("1000reduced")


  }
}