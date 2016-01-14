package au.csiro.obr17q.variantspark

import au.csiro.obr17q.variantspark.CommonFunctions._
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import scala.io.Source
import au.csiro.obr17q.variantspark.algo.WideKMeans
import au.csiro.pbdava.sparkle.LoanUtils
import com.github.tototoshi.csv.CSVReader
import java.io.File
import java.io.FileReader
import com.github.tototoshi.csv.CSVWriter
import au.csiro.obr17q.variantspark.algo.WideDecisionTree
import au.csiro.obr17q.variantspark.algo.WideRandomForest
import au.csiro.obr17q.variantspark.algo.RandomForestParams
import scala.collection.mutable.MutableList

object SparseWideForest extends SparkApp {
  conf.setAppName("VCF cluster")
  //conf.registerKryoClasses(Array(classOf[TreeSplitInfo]))
  
  def main(args:Array[String]) {

   
    if (args.length < 1) {
        println("Usage: CsvClusterer <input-path>")
    }

    val inputFiles = args(0)
    val output = args(1)
    val ntree = args(2).toInt
    val ntryfraction = if (args.length > 3) args(3).toDouble else Double.NaN
        
    val sparseVariant = sqlContext.read.parquet(inputFiles)
    println(sparseVariant.schema)

   val indexSubjectMap = LoanUtils.withCloseable(CSVReader.open(new FileReader(new File(inputFiles, "_index.csv")))){
      csvReader =>
        csvReader.iterator.map { x => (x(1).toInt,x(0))}.toMap   
    }    
    
    val PopFiles = Source.fromFile("data/ALL.panel").getLines()
    val Populations = sc.parallelize(new MetaDataParser(PopFiles, 1, '\t', "NA", 0, 1 ).returnMap(Array(), Array()))
    
    val SuperPopulationUniqueId = Populations.map(_.SuperPopulationId).distinct().zipWithIndex() //For ARI
    val superPopToId = SuperPopulationUniqueId.collectAsMap()
    val subjectToSuperPopId= Populations.map(ind => (ind.IndividualId, superPopToId(ind.SuperPopulationId))).collectAsMap()

    val allLabels = indexSubjectMap.toStream.sorted
      .map({case (index,subject) => 
        subjectToSuperPopId.getOrElse(subject, superPopToId.size.toLong).toInt
      }).toArray
    
   
   val unknownLabel = superPopToId.size
   val unknownLabelsCount = allLabels.count( _ == unknownLabel)
   println(s"Unknown labels count: ${unknownLabelsCount}")
   // need to get of unknow data   
      
   val indexCorrections = Array.fill(allLabels.length)(0)
  
   // I am sure there is a way to do this in a nice functional programming way but for now
   var counts = 0;
   allLabels.zipWithIndex.foreach { case(v,i) => 
     if (v == unknownLabel) {
       counts += 1
     }
     indexCorrections(i) = counts
   }
   
   
   val vectorData = 
      sparseVariant.rdd
        .map{r=> 
          val (size, indexes,values) = (r.getInt(1),r.getSeq[Int](2).toArray, r.getSeq[Double](3))
          val filteredIndexs = MutableList[Int]()
          Vectors.sparse(size - unknownLabelsCount,
          indexes.filter(i => allLabels(i) < unknownLabel).map(i => i - indexCorrections(i)).toArray,
          values.zipWithIndex.filter({ case(v,i) => allLabels(indexes(i)) < unknownLabel }).map(_._1).toArray)
         }
   
   
    val data = vectorData.zipWithIndex().cache()
    val test = data.count()
    println(test)    
    
    val labels = allLabels.filter(_ < unknownLabel)
    
    val startTime = System.currentTimeMillis()
    val rf = new WideRandomForest()
    val result  = rf.run(data,labels.toArray, ntree, RandomForestParams(oob=true, nTryFraction = ntryfraction))
    //println(result)
    val runTime = System.currentTimeMillis() - startTime
    println(s"Run time: ${runTime}")
    //result.printout()
    val variableImportnace = result.variableImportance
    
    variableImportnace.toSeq.sortBy(-_._2).take(20).foreach(println)
    
    
    //LoanUtils.withCloseable(CSVWriter.open(output)) { cswWriter =>
    //  clusterAssignment.zipWithIndex.map{ case (cluster,index) => (indexSubjectMap(index), cluster)}
    //    .foreach(t => cswWriter.writeRow(t.productIterator.toSeq))
    //}
  
  } 
}