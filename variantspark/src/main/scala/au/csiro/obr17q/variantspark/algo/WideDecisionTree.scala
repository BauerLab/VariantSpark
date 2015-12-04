package au.csiro.obr17q.variantspark.algo

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.util.random.XORShiftRandom
import org.apache.spark.mllib.linalg.Vector
import it.unimi.dsi.fastutil.ints.Int2DoubleOpenHashMap
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap
import collection.JavaConversions._

object WideDecisionTree {

  def sqr(x:Double) = x*x
  
  def giniImprity(counts:Array[Int]):Double = {
     val total = counts.sum.toDouble
     if (total == 0.0)  0.0 else  1 - counts.map(s => sqr(s/total)).sum
  } 

  
  def findSplit(currentSet:Array[Int], labels:Array[Int])(t:(Vector,Long)):(Double,Long,Int,Int, Array[Int], Array[Int]) = {
   
    val v = t._1.toArray
    val index:Long = t._2
    val labelsCount = labels.max+1
    
    val labelCounts = Array.fill(labelsCount)(0)
    currentSet.foreach(i => labelCounts(labels(i))+=1)
    val totalGini = giniImprity(labelCounts)
    
    // calculat the actual information gain for each split
    val majorityLabel = labelCounts.zipWithIndex.max._2
 
    // now this wouild be done fastest with a contingency table but with 3 values
    // lets to a simple approach first
        
    //assuming values are 0, 1, 2 there are two possible splits
    // left side split x<=i
    val (splitGini, s) = Range(0,2).map{ s =>      
          val leftCount = Array.fill(labelsCount)(0)
          val rightCount = Array.fill(labelsCount)(0)
          currentSet.foreach(i => if (v(i) <=s)  leftCount(labels(i))+=1 else rightCount(labels(i))+=1)
          
          val leftItems = leftCount.sum
          val rightItem = rightCount.sum
          val splitGini = (giniImprity(leftCount) * leftItems.toDouble +  giniImprity(rightCount) * rightItem.toDouble)/(leftItems + rightItem)
          (splitGini, s)
    }.min
    // calculat the best ginit split
    // actually also need to return new subsplits
    (totalGini - splitGini,index,s, majorityLabel, currentSet.filter(i => v(i)<=s),  currentSet.filter(i => v(i)>s))
  }
}

case class DecisionTreeNode(variableIndex:Long, splitPoint:Int,majorityLabel:Int, impurityReduction:Double, size:Int,left:DecisionTreeNode = null, right:DecisionTreeNode = null) {
 
  
  def isLeaf = (impurityReduction == 0)
  def printout(level:Int=0) {
    print(new String(Array.fill(level)(' ')))
    val nodeType = if (isLeaf) "leaf" else "split"
    println(s"${nodeType}[${variableIndex}, ${splitPoint}, ${majorityLabel}, ${size}, ${impurityReduction}]")
    if (!isLeaf) {
      left.printout(level + 1)
      right.printout(level + 1)
    }
  }
  
  
  def countImportance(accumulations:Long2DoubleOpenHashMap) {
      if (!isLeaf) {
           accumulations.addTo(variableIndex,impurityReduction)
      }
  }
  
  def variableImportanceAsFastMap:Long2DoubleOpenHashMap = {
    val accumulations =  new Long2DoubleOpenHashMap();
    countImportance(accumulations)
    accumulations
  }
  
  def variableImportance():Map[Long, Double] = {
    variableImportanceAsFastMap.entrySet().map(e => (e.getKey.toLong, e.getValue.toDouble)).toMap
  }
  
}


class WideDecisionTree {
  
    def predict(data: RDD[Vector], tree:DecisionTreeNode):Array[Int]  = {
      // this is a bit tricky but say lets' collect all the values neeed to resolve the thre
      
      //map the tree into a set of indexes
      
      def mapTrees(tree:DecisionTreeNode):List[Long] = {
        if (tree.impurityReduction > 0 && tree.left!=null && tree.right!=null ) tree.variableIndex :: mapTrees(tree.left) ::: mapTrees(tree.right)
        else List()
      }
      
      val indexes = mapTrees(tree).toSet
      // now collect values of selected index
      val points = data.zipWithIndex.filter{ case(v,i) => indexes.contains(i)}.map(_.swap).collectAsMap()
      
      val tmp = Array.fill(data.first().size)(tree)
      while(tmp.exists { x => x.impurityReduction >0 }) {
        Range(0,tmp.length).foreach { i =>
          val tn = tmp(i)
          if (!tn.impurityReduction.isNaN() && tn.right!=null && tn.left!=null) {
            tmp(i) = if (points(tn.variableIndex)(i)<=tn.splitPoint) tn.left else tn.right
          }
        }
      }
      tmp.map(_.majorityLabel)
    }
    
    
    def run(data: RDD[Vector], labels:Array[Int]):DecisionTreeNode = run(data,labels, Range(0,data.first().size).toArray)
   
    def run(data: RDD[Vector], labels:Array[Int], currentSet:Array[Int]):DecisionTreeNode = {
         
      val c = data.count()
      val nvarFraction:Double  = 0.1 //Math.sqrt(c.toDouble)/c.toDouble
      
      val indexedData = data.zipWithIndex()
        // what we need to do it so select variables for each      
        // sample a few variables.
        // indexes of elements included in current split
        buildSplit(indexedData,currentSet, labels, nvarFraction) 
    } 
  
    def buildSplit(indexedData: RDD[(Vector,Long)], currentSet:Array[Int], labels:Array[Int], nvarFraction:Double):DecisionTreeNode = {
        // for the current set find all candidate splits
        
        val (giniReduction, varIndex, split, majorityLabel, leftSet,rightSet) = indexedData.sample(false, nvarFraction, (Math.random()*10000).toLong) // sample the variables (should be sqrt(n)/n for classification)
          .map(WideDecisionTree.findSplit(currentSet, labels))
          .reduce((f1,f2) => if (f1._1 > f2._1) f1 else f2) // dumb way to use minimum
      
         // check if futher split is needed
        
         //println("Gini reduction:" + giniReduction) 
          
         if (giniReduction > 0) {
           DecisionTreeNode(varIndex, split, majorityLabel, giniReduction,currentSet.length,buildSplit(indexedData,leftSet, labels, nvarFraction)
               ,buildSplit(indexedData,rightSet, labels, nvarFraction))
         } else {
           DecisionTreeNode(varIndex, split, majorityLabel, giniReduction,currentSet.length, null, null)           
         } 
    }
}


case class WideRandomForestModel(trees:List[DecisionTreeNode]) {
  def printout() {
    trees.zipWithIndex.foreach { case (tree, index) => 
      println(s"Tree: ${index}")
      tree.printout()
      }
  }
  
  def variableImportance:Map[Long,Double] = {
    // average by all trees
    val accumulations =  new Long2DoubleOpenHashMap();
    val counts =  new Long2IntOpenHashMap();
    trees.foreach { t =>
      val treeImportnace = t.variableImportanceAsFastMap
      treeImportnace.foreach { case (index, imp) =>
        accumulations.addTo(index, imp)
        counts.addTo(index,1)
      }
    }
    accumulations.map{ case (index,value) => (index.toLong, value/counts.get(index))}.toMap
  }
}


class WideRandomForest {
  def run(data: RDD[Vector], labels:Array[Int], ntrees:Int):WideRandomForestModel = {
    // subsample 
    val dims = labels.length
    val trees = Range(0,ntrees).map { i =>
      println(s"Building tree: ${i}")
      val currentSample = for (i <- 0 to dims) yield (Math.random()*dims).toInt // sample with replacemnt
      new WideDecisionTree().run(data,labels,currentSample.toArray)
    } 
    WideRandomForestModel(trees.toList)
  }
}




