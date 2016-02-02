package au.csiro.obr17q.variantspark.model

import au.csiro.obr17q.variantspark.IndividualMap
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

/**
 * @author obr17q
 */

class ThousandGenomesVcfParser (override val VcfFileNames: String, override val VariantCutoff: Int, override val IndividualMeta: RDD[IndividualMap],
                                override val sc: SparkContext, override val sqlContext: SQLContext) extends
  VcfParser(VcfFileNames, VariantCutoff, IndividualMeta, sc, sqlContext) {

  override def data = sqlContext
    .createDataFrame {
      val variantCount = this.variantCount
      println(variantCount)
      individualTuples
        .groupByKey //group by individual ID, i.e. get RDD of individuals
        .join(IndividualMeta.map(_.toIndo)) //filter out individuals lacking required data
        .map(h => VcfRecord(
          individual = h._1,
          population = h._2._2._1,
          superPopulation = h._2._2._2,
          features = Vectors.sparse(variantCount, h._2._1.to[Seq]))
        )
    }
}


