package au.csiro.obr17q.variantspark

import au.com.bytecode.opencsv.CSVParser
import au.csiro.obr17q.variantspark.CommonFunctions._
/**
 * @author obr17q
 */


class MetaDataParser(val PopFiles:Iterator[String], val HeaderLines:Int, val FileDeliminator:Char, val NullString:String,
                      val IndividualIdCol:Int, val PopulationCol:Int)
                   (implicit val WeightCol: Int = -1, HeightCol: Int = -1, SexCol: Int = -1,
                      extra1: Int = PopulationCol, val extra2: Int = PopulationCol, val extra3: Int = PopulationCol, val extra4: Int = PopulationCol) {

  val Populations = (PopFiles
      .toList
      .drop(HeaderLines)
      .map ( line => {
        val parser = new CSVParser(FileDeliminator, '"')
        val l = parser.parseLine(line)
        val individualId = l(IndividualIdCol)
        val Population = l(PopulationCol)
        val SuperPopulation = l(extra1)
        //val SuperPopulation = popmap.pops(Population)
        //( l(IndividualIdCol), l(PopulationCol) ) // (IndividualID, Population)
        
        
        val sex = if (SexCol >= 0 && l(SexCol) == "FEMALE") 0 else if (SexCol >= 0 && l(SexCol) == "MALE") 1 else -1
        val height = if (HeightCol >= 0 && l(HeightCol) != NullString) l(HeightCol).toDouble else 0
        val weight = if (WeightCol >= 0 && l(WeightCol) != NullString) l(WeightCol).toDouble else 0
        val bmi = getBmi(height, weight)
        
        
        new IndividualMap(individualId, Population, SuperPopulation) (l(extra1), l(extra2), l(extra3), l(extra4)) (weight, height, bmi) (sex)
      } )
      //.filter(IndividualInfo => ( (IndividualInfo.AdditionalOne == IndividualInfo.AdditionalTwo) && (IndividualInfo.AdditionalOne == IndividualInfo.AdditionalThree) && (IndividualInfo.AdditionalOne == IndividualInfo.AdditionalFour) ))
  )

  def returnMap(IncludeGroups:Array[String] = Array(""), ExcludeGroups:Array[String] = Array("") ):List[IndividualMap] = {
    return Populations.filter(IndividualInfo => (IndividualInfo.filterGroups(IncludeGroups, ExcludeGroups))).toList
  }
  def returnPopMap(IncludeGroups:Array[String] = Array(""), ExcludeGroups:Array[String] = Array("") ):List[IndividualMap] = {
    return Populations.filter(IndividualInfo => (IndividualInfo.filterGroups(IncludeGroups, ExcludeGroups))).toList
  }
  def returnBmiMap( ):List[IndividualMap] = {
    return Populations.filter(IndividualInfo => IndividualInfo.BMI > 0).toList
  }
  def returnSexMap( ):List[IndividualMap] = {
    return Populations.filter(IndividualInfo => IndividualInfo.Sex > -1).toList
  }
}