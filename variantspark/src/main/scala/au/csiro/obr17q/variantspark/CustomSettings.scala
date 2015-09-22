package au.csiro.obr17q.variantspark

/**
 * @author obr17q
 */

class BaseMap(val superPopId: String, val popArray: Array[String])
class SuperPopulationMap(val pops: Map[String, String])

object BaseMap {
  def apply(superPopId: String, popArray: Array[String]) = new BaseMap(superPopId, popArray)
}

object CustomSettings {

  val PGPpopulationArray =
            BaseMap("EUR", Array("Sweden", "Finland", "Ireland", "France", "Germany", "Greece", "United Kingdom", "Spain", "Poland", "Belgium")) ::
            BaseMap("SAS", Array("India","Bangladesh")) ::
            BaseMap("AMR", Array("United States Minor Outlying Islands",  "Canada")) ::
            BaseMap("EUE", Array("Bulgeria", "Ukraine", "Estonia", "Slovenia", "Russian Federation")) ::
            BaseMap("MXL", Array("Puerto Rico", "Mexico")) ::
            BaseMap("AFR", Array("South Africa")) ::
            BaseMap("ASN", Array("China", "Taiwan, Province of China")) ::
            Nil


        val TGPpopulationArray =
            BaseMap("MEX", Array()) ::
            BaseMap("EUR", Array("GBR", "FIN", "IBS", "CEU", "TSI")) ::
            BaseMap("EAS", Array("CHS", "CHB", "CDX", "JPT", "KHV" )) ::
            BaseMap("AMR", Array("CLM", "MXL", "PEL", "PUR" )) ::
            BaseMap("SAS", Array("BEB", "GIH", "STU", "ITU", "PJL")) ::
            BaseMap("AFR", Array("ACB", "ASW", "GWD", "ESN", "LWK", "MSL", "YRI")) ::
            Nil
        
        implicit val popmap = new SuperPopulationMap(
            TGPpopulationArray.map( BaseMap => {
              BaseMap.popArray.map(PopId => (PopId, BaseMap.superPopId))
            }).flatMap(popTuple => popTuple.map(p => (p._1, p._2))).toMap
        )
}