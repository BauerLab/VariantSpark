package au.csiro.obr17q.variantspark

import au.com.bytecode.opencsv.CSVParser
import au.csiro.obr17q.variantspark.CustomSettings._

/**
 * @author obr17q
 */
class IndividualMap(val IndividualId: String, val PopulationId: String, val SuperPopulationId: String = "")
      (val AdditionalOne: String = "", val AdditionalTwo: String = "", val AdditionalThree: String = "", val AdditionalFour: String = "") extends java.io.Serializable {
  def toTup = (PopulationId, (IndividualId, AdditionalOne, AdditionalTwo, AdditionalThree, AdditionalFour))
  def toMap = Map(PopulationId -> (IndividualId, AdditionalOne, AdditionalTwo, AdditionalThree, AdditionalFour))
  def toIndo = (IndividualId, (PopulationId,SuperPopulationId))
  def toIndoAll = (IndividualId, (PopulationId, SuperPopulationId, AdditionalOne, AdditionalTwo, AdditionalThree, AdditionalFour))

  def filterGroups(include:Array[String], exclude:Array[String]):Boolean = {
    val to_include = if (include.size > 0 && include(0) != "") include.contains(PopulationId) else true
    val to_exclude = if (exclude.size > 0 && exclude(0) != "") !exclude.contains(PopulationId) else true
    if (to_include == false || to_exclude == false) false else true
  }
}

class PopulationMap(val PopFiles:Iterator[String], val header:String,
                    val FileDeliminator:Char, val IndividualIdCol:Int, val PopulationCol:Int)
                    (implicit val e1: Int = PopulationCol, val e2: Int = PopulationCol, val e3: Int = PopulationCol, val e4: Int = PopulationCol) {
  val too = PopFiles.toList
  val Populations = (too
    .filter( !_.startsWith(header) )
    .map ( lines => {
      val parser = new CSVParser(FileDeliminator, '"')
      val l = parser.parseLine(lines)
      val individualId = l(IndividualIdCol)
      val Population = l(PopulationCol)
      //val SuperPopulation = popmap.pops(Population)
      //( l(IndividualIdCol), l(PopulationCol) ) // (IndividualID, Population)
      new IndividualMap(individualId, Population, Population)( l(e1), l(e2), l(e3), l(e4))
    } )
    //.filter(IndividualInfo => ( (IndividualInfo.AdditionalOne == IndividualInfo.AdditionalTwo) && (IndividualInfo.AdditionalOne == IndividualInfo.AdditionalThree) && (IndividualInfo.AdditionalOne == IndividualInfo.AdditionalFour) ))
  )

  def returnMap(IncludeGroups:Array[String] = Array(""), ExcludeGroups:Array[String] = Array("") ):Iterator[IndividualMap] = {
    return Populations.filter(IndividualInfo => (IndividualInfo.filterGroups(IncludeGroups, ExcludeGroups))).toIterator
  }

}