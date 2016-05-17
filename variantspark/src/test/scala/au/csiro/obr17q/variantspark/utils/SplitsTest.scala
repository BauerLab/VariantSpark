package au.csiro.obr17q.variantspark.utils


import org.junit.Assert._
import org.junit.Test;
import org.apache.spark.mllib.linalg.Vectors

class SamplingTests {
  
  @Test 
  def testSubsample() {
    val sample:Array[Int] = Sampling.subsample(10, 5)
    assertEquals(5, sample.length)
    assertTrue(sample.toList.forall { i => i > 0 && i < 10 })
  }
  
  @Test
  def testFolds() {   
    val folds = Sampling.folds(10, 3)
    assertEquals(3, folds.size)
    assertTrue(folds.map(_.size).forall(_ >=3))
    assertEquals(10, folds.map(_.size).reduce(_+_))
    assertEquals(10, folds.map(_.toSet).reduce(_++_).size)
    assertEquals(Range(0,10).toSet, folds.map(_.toSet).reduce(_++_))
  }
  
  
}


class SplitsTest {
  
  @Test
  def testFolds() {
    val v = Vectors.zeros(10)
    val a = Array.range(0, 10)
    val folds = Projector.folds(v,4)
    folds.foreach { x => println(x.projectArray(a).toList, x.inverted.projectArray(a).toList) }
  }
 
  @Test
  def testSplits() {
    val v = Vectors.zeros(10)
    val a = Array.range(0, 10)
    val (train,test) = Projector.split(v, 0.8)
    println(train.projectArray(a).toList, test.projectArray(a).toList)
  }
}