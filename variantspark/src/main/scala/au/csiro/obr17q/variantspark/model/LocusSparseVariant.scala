package au.csiro.obr17q.variantspark.model

case class LocusSparseVariant(val locusId:Int, val size:Int, val indices:Array[Int], val values:Array[Double]) {

}