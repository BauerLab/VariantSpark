package au.csiro.obr17q.variantspark

/**
 * @author obr17q
 */
class IndividualMap (val IndividualId: String, val PopulationId: String, val SuperPopulationId: String = "")
(val AdditionalOne: String = "", val AdditionalTwo: String = "", val AdditionalThree: String = "", val AdditionalFour: String = "")
(val Weight: Double = 0, val Height: Double = 0, val BMI: Double = 0)
(val Sex: Int = 0)
extends java.io.Serializable {
  
  def toTup = (PopulationId, (IndividualId, AdditionalOne, AdditionalTwo, AdditionalThree, AdditionalFour))
  def toMap = Map(PopulationId -> (IndividualId, AdditionalOne, AdditionalTwo, AdditionalThree, AdditionalFour))
  def toIndo = (IndividualId, (PopulationId,SuperPopulationId))
  def toPops = (IndividualId, (PopulationId))
  def toBMI = (IndividualId, (BMI))
  def toSex = (IndividualId, (Sex))
  def toIndoAll = (IndividualId, (PopulationId, SuperPopulationId, AdditionalOne, AdditionalTwo, AdditionalThree, AdditionalFour))
  
  
  /**
   * filterGroups returns a boolean for whether to keep (true) an individual or not (false).
   * include is an array of individuals to keep. Individuals not in this array will be removed.
   * exclude is an array of individuals to remove. Individuals not in this array will be kept. 
   * If the input arrays are empty, returns true.
   */
  def filterGroups(include:Array[String], exclude:Array[String]):Boolean = {
    val to_include = if (include.size > 0 && include(0) != "") include.contains(PopulationId) else true
    val to_exclude = if (exclude.size > 0 && exclude(0) != "") !exclude.contains(PopulationId) else true
    if (to_include == false || to_exclude == false) false else true
  }
}