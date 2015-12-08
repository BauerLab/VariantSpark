package au.csiro.obr17q.variantspark.algo

import scala.Range
import scala.collection.JavaConversions.asScalaSet

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap

object WideDecisionTree {

  def projectVector(indexSet: Set[Int], invert: Boolean = false)(v: Vector): Vector = {
    val a = v.toArray
    Vectors.dense((for (i <- 0 until a.length if (indexSet.contains(i) == !invert)) yield a(i)).toArray)
  }

  def projectArray(indexSet: Set[Int], invert: Boolean = false)(a: Array[Int]): Array[Int] = {
    (for (i <- 0 until a.length if (indexSet.contains(i) == !invert)) yield a(i)).toArray
  }

  def sqr(x: Double) = x * x

  def giniImprity(counts: Array[Int]): Double = {
    val total = counts.sum.toDouble
    if (total == 0.0) 0.0 else 1 - counts.map(s => sqr(s / total)).sum
  }

  def findSplit(currentSet: Array[Int], labels: Array[Int])(t: (Vector, Long)): (Double, Double, Long, Int, Int, Array[Int], Array[Int]) = {

    val v = t._1.toArray
    val index: Long = t._2
    val labelsCount = labels.max + 1

    val labelCounts = Array.fill(labelsCount)(0)
    currentSet.foreach(i => labelCounts(labels(i)) += 1)
    val totalGini = giniImprity(labelCounts)

    // calculat the actual information gain for each split
    val majorityLabel = labelCounts.zipWithIndex.max._2

    // now this wouild be done fastest with a contingency table but with 3 values
    // lets to a simple approach first

    //assuming values are 0, 1, 2 there are two possible splits
    // left side split x<=i
    val (splitGini, s) = Range(0, 2).map { s =>
      val leftCount = Array.fill(labelsCount)(0)
      val rightCount = Array.fill(labelsCount)(0)
      currentSet.foreach(i => if (v(i) <= s) leftCount(labels(i)) += 1 else rightCount(labels(i)) += 1)

      val leftItems = leftCount.sum
      val rightItem = rightCount.sum
      val splitGini = (giniImprity(leftCount) * leftItems.toDouble + giniImprity(rightCount) * rightItem.toDouble) / (leftItems + rightItem)
      (splitGini, s)
    }.min
    // calculat the best ginit split
    // actually also need to return new subsplits
    (totalGini - splitGini, totalGini, index, s, majorityLabel, currentSet.filter(i => v(i) <= s), currentSet.filter(i => v(i) > s))
  }
}

case class DecisionTreeNode(variableIndex: Long, splitPoint: Int, majorityLabel: Int,
    impurityReduction: Double, nodeImpurity: Double, size: Int, left: DecisionTreeNode = null, right: DecisionTreeNode = null) {

  def isLeaf = (impurityReduction == 0)

  def printout(level: Int) {
    print(new String(Array.fill(level)(' ')))
    val nodeType = if (isLeaf) "leaf" else "split"
    println(s"${nodeType}[${variableIndex}, ${splitPoint}, ${majorityLabel}, ${size}, ${impurityReduction}, ${nodeImpurity}]")
    if (!isLeaf) {
      left.printout(level + 1)
      right.printout(level + 1)
    }
  }

  def countImportance(accumulations: Long2DoubleOpenHashMap, totalSize:Int) {
    if (!isLeaf) {
      
      accumulations.addTo(variableIndex, (size*nodeImpurity - (left.size*left.nodeImpurity + right.size*right.nodeImpurity))/totalSize.toDouble)
      left.countImportance(accumulations, totalSize)
      right.countImportance(accumulations, totalSize)
    }
  }

}

class WideDecisionTreeModel(val rootNode: DecisionTreeNode) {

  def predict(data: RDD[Vector]): Array[Int] = {
    // this is a bit tricky but say lets' collect all the values neeed to resolve the thre

    //map the tree into a set of indexes

    def mapTrees(tree: DecisionTreeNode): List[Long] = {
      if (tree.impurityReduction > 0 && tree.left != null && tree.right != null) tree.variableIndex :: mapTrees(tree.left) ::: mapTrees(tree.right)
      else List()
    }

    val indexes = mapTrees(rootNode).toSet
    // now collect values of selected index
    val points = data.zipWithIndex.filter { case (v, i) => indexes.contains(i) }.map(_.swap).collectAsMap()

    val tmp = Array.fill(data.first().size)(rootNode)
    while (tmp.exists { x => x.impurityReduction > 0 }) {
      Range(0, tmp.length).foreach { i =>
        val tn = tmp(i)
        if (!tn.impurityReduction.isNaN() && tn.right != null && tn.left != null) {
          tmp(i) = if (points(tn.variableIndex)(i) <= tn.splitPoint) tn.left else tn.right
        }
      }
    }
    tmp.map(_.majorityLabel)
  }

  def printout() {
    rootNode.printout(0)
  }

  def variableImportanceAsFastMap: Long2DoubleOpenHashMap = {
    val accumulations = new Long2DoubleOpenHashMap();
    rootNode.countImportance(accumulations, rootNode.size)
    accumulations
  }

  def variableImportance(): Map[Long, Double] = {
    variableImportanceAsFastMap.entrySet().map(e => (e.getKey.toLong, e.getValue.toDouble)).toMap
  }

}

class WideDecisionTree {
  def run(data: RDD[Vector], labels: Array[Int]): WideDecisionTreeModel = run(data, labels, Range(0, data.first().size).toArray)
  def run(data: RDD[Vector], labels: Array[Int], currentSet: Array[Int]): WideDecisionTreeModel = {

    val c = data.count()
    val nvarFraction: Double = 0.3 //Math.sqrt(c.toDouble)/c.toDouble

    val indexedData = data.zipWithIndex()
    // what we need to do it so select variables for each      
    // sample a few variables.
    // indexes of elements included in current split
    new WideDecisionTreeModel(buildSplit(indexedData, currentSet, labels, nvarFraction))
  }

  def buildSplit(indexedData: RDD[(Vector, Long)], currentSet: Array[Int], labels: Array[Int], nvarFraction: Double): DecisionTreeNode = {
    // for the current set find all candidate splits

    val (giniReduction, totalGini, varIndex, split, majorityLabel, leftSet, rightSet) = indexedData.sample(false, nvarFraction, (Math.random() * 10000).toLong) // sample the variables (should be sqrt(n)/n for classification)
      .map(WideDecisionTree.findSplit(currentSet, labels))
      .reduce((f1, f2) => if (f1._1 > f2._1) f1 else f2) // dumb way to use minimum

    // check if futher split is needed

    //println("Gini reduction:" + giniReduction) 

    if (giniReduction > 0) {
      DecisionTreeNode(varIndex, split, majorityLabel, giniReduction, totalGini, currentSet.length, buildSplit(indexedData, leftSet, labels, nvarFraction), buildSplit(indexedData, rightSet, labels, nvarFraction))
    } else {
      DecisionTreeNode(varIndex, split, majorityLabel, giniReduction, totalGini, currentSet.length, null, null)
    }
  }
}




