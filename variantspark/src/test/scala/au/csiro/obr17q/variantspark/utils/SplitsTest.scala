package au.csiro.obr17q.variantspark.utils


import org.junit.Assert._
import org.junit.Test;
import org.apache.spark.mllib.linalg.Vectors

class SplitsTest {
  
  @Test
  def testFolds() {
    val v = Vectors.zeros(10)
    val a = Array.range(0, 10)
    val folds = Splits.folds(v,4)
    folds.foreach { x => println(x.projectArray(a).toList, x.inverted.projectArray(a).toList) }
  }
 
  @Test
  def testSplits() {
    val v = Vectors.zeros(10)
    val a = Array.range(0, 10)
    val (train,test) = Splits.split(v, 0.8)
    println(train.projectArray(a).toList, test.projectArray(a).toList)
  }
  
  
  
}