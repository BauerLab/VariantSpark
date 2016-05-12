package au.csiro.obr17q.variantspark.utils


import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.commons.math3.random.RandomDataGenerator
import org.apache.spark.rdd.RDD


class Projector(indexSet: Set[Int], invert: Boolean = false) extends Serializable {

  def projectVector(v: Vector): Vector = {
    val a = v.toArray
    Vectors.dense((for (i <- a.indices if indexSet.contains(i) == !invert) yield a(i)).toArray)
  }

  def projectArray(a: Array[Int]): Array[Int] = {
    (for (i <- a.indices if indexSet.contains(i) == !invert) yield a(i)).toArray
  }
  
  def apply(rdd:RDD[Vector]):RDD[Vector] = rdd.map(projectVector)
  
  def inverted = new Projector(indexSet, !invert)
  
  def indexes = indexSet
}
  
  
object Splits {
  
  def subsample(v:Vector, fraction:Double):Projector = {
    val rdg = new RandomDataGenerator()
    val size = v.size
    val sampleSize = math.round(size*fraction).toInt
    val includedIndexs = rdg.nextPermutation(size, sampleSize)
    return new Projector(includedIndexs.toSet, false)
  }
  
//  def split(rdd: RDD[Vector], fraction:Double):(RDD[Vector],RDD[Vector]) = {
//    val projector = subsample(rdd.first, fraction)
//    return (rdd.map(projector.projectVector),rdd.map(projector.inverted.projectVector)) 
//  }
 
  def split(v:Vector, fraction:Double):(Projector, Projector) = {
    val projector = subsample(v, fraction)
    return (projector, projector.inverted) 
  }
  
  def splitRDD(rdd: RDD[Vector], fraction:Double):(Projector, Projector) = split(rdd.first, fraction)
  
  def folds(v:Vector, nFolds:Int, testFolds:Boolean = true):List[Projector] = {
    val rdg = new RandomDataGenerator()
    val size = v.size
    //check it there is enough to have at least one element per folr
    if (size < nFolds)
      throw new RuntimeException("Not enougth elements")
    val permutation = rdg.nextPermutation(size, size)
    // now I just need to distribute this collections evenly into n folds
    val byFold = Array.range(0, size).groupBy(permutation(_)%nFolds)
    // we do not really care which order we get the folds in
    byFold.values.map(a => new Projector(a.toSet, !testFolds)).toList
  }
  
  def rddFolds(rdd:RDD[Vector], nFolds:Int, testFolds:Boolean = true):List[Projector] = folds(rdd.first, nFolds, testFolds)
  
  // so essentially we want to be able to split a RDD[Vector] into two 
  // mutually exclusive sets based on a set of random indexes
}