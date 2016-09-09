package au.csiro.obr17q.variantspark.model

import au.csiro.obr17q.variantspark.IndividualMap
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

/**
 * @author obr17q
 */

class GenericVcfParser(override val VcfFileNames: String, override val VariantCutoff: Int, override val IndividualMeta: RDD[IndividualMap],
                       override val sc: SparkContext, override val sqlContext: SQLContext) extends
  VcfParser(VcfFileNames, VariantCutoff, IndividualMeta, sc, sqlContext) {


  /**
    * @return DataFrame with columns "individual" and "features"
    */
  override def data = sqlContext
    .createDataFrame {
      val variantCount = this.variantCount
      println(variantCount)
      individualTuples
        .groupByKey //group by individual ID, i.e. get RDD of individuals
        .map(h => VcfRecord(
          individual = h._1,
          features = Vectors.sparse(variantCount, h._2.to[Seq]))
        )
    }
}


