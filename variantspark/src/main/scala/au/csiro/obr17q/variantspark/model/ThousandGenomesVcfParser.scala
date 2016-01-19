package au.csiro.obr17q.variantspark.model

import au.com.bytecode.opencsv.CSVParser
import au.csiro.obr17q.variantspark.CommonFunctions.variantDist
import au.csiro.obr17q.variantspark.IndividualMap
import au.csiro.obr17q.variantspark.VcfForest._
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg._
import org.apache.spark.rdd.RDD

/**
 * @author obr17q
 */

class ThousandGenomesVcfParser (override val VcfFileNames: String, override val VariantCutoff: Int, override val IndividualMeta: RDD[IndividualMap], override val sc: SparkContext) extends
  VcfParser(VcfFileNames, VariantCutoff, IndividualMeta, sc) {



  //(IndividualName, Vector)
  override lazy val data =
    sqlContext
      .createDataFrame {
        individualTuples
          .groupByKey //group by individual ID, i.e. get RDD of individuals
          .join(IndividualMeta.map(_.toIndo)) //filter out individuals lacking required data
          .map(h =>
          VcfRecord(
            individual = h._1,
            population = h._2._2._1,
            superPopulation = h._2._2._2,
            features = Vectors.sparse(variantCount, h._2._1.to[Seq]))
        )
      }
}


