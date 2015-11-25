package au.csiro.obr17q.variantspark.model

case class SubjectSparseVariant(val subjectId:String, val size:Int, val indices:Array[Int], val values:Array[Double]) {

}