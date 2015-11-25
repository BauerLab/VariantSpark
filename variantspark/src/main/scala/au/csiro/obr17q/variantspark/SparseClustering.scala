package au.csiro.obr17q.variantspark

import au.csiro.obr17q.variantspark.CommonFunctions._
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors

import scala.io.Source

object SparseClustering extends SparkApp {
  conf.setAppName("VCF cluster")
  def main(args:Array[String]) {

   
    if (args.length < 1) {
        println("Usage: CsvClusterer <input-path>")
    }

    val inputFiles = args(0)
    val k = args(1).toInt
    val IncludeGroups = args(2).split('|')
    val ExcludeGroups = args(3).split('|')
    val VariantCutoff = args(4).toInt

    
    
    
    
    //val PopFiles = Source.fromFile("data/PGPParticipantSurvey-20150831064509.csv").getLines()
    //val Populations = sc.parallelize(new PopulationMap(PopFiles, 1, ',', 0, 16 ).returnMap(IncludeGroups, ExcludeGroups))

    val PopFiles = Source.fromFile("../data/ALL.panel").getLines()
    val Populations = sc.parallelize(new MetaDataParser(PopFiles, 1, '\t', "NA", 0, 1 ).returnMap(IncludeGroups, ExcludeGroups))

    
    val pops = Populations.map(_.toPops).collectAsMap();
    println(pops)
    val pops_br = sc.broadcast(pops);
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    val sparseVariat = sqlContext.parquetFile(inputFiles)    
    println(sparseVariat.schema)

    
    val IndividualVariants = 
      sparseVariat.rdd
        .filter(r => pops_br.value.contains(r.getString(0)))
        .map(r=> (pops_br.value(r.getString(0)), r.getString(0), Vectors.sparse(r.getInt(1),
          r.getSeq[Int](2).toArray, r.getSeq[Double](3).toArray)))
      
      
      


    /** Print populations included and number of individuals (slow) **/
    //IndividualVariants.map(p => (p._1, 1)).join(Populations.map(_.toPops)).map(p => (p._2._2, 1)).reduceByKey(_ + _).collect().foreach(println)

    /** Print the count of variants for each individual **/
    //val countt = SparseVariants.map(p => (p._2.toArray).reduce(_ + _)).collect().foreach(println)

    //Populations.collect().foreach(println)
    //println("Processed VCF file with %s variants.".format(NoOfAlleles))
    
    val pEndTime = System.currentTimeMillis()






    //// val SparVecs = rows
    //val mat: RowMatrix = new RowMatrix(SparVecs.map(_._2))
    //// Compute similar columns perfectly, with brute force.
    //val exact = mat.columnSimilarities()
    //// Compute similar columns with estimation using DIMSUM
    //val approx = mat.columnSimilarities(0.1)
    //val exactEntries = exact.entries.map { case MatrixEntry(i, j, u) => ((i, j), u) }
    //val approxEntries = approx.entries.map { case MatrixEntry(i, j, v) => ((i, j), v) }
    //val MAE = exactEntries.leftOuterJoin(approxEntries).values.map {
    //  case (u, Some(v)) =>
    //    math.abs(u - v)
    //  case (u, None) =>
    //    math.abs(u)
    //}.mean()
    //println(s"Average absolute error in estimate is: $MAE")


     //val dataFrame = SparseVariants.map(p => (p._2) )


    //val SparseVariants: RDD[(String, String, Vector)] = sc.objectFile("/flush/obr17q/phase3RDD")
    
    val dataFrame = IndividualVariants.cache()
    //val dataFrame: RDD[Vector] = sc.objectFile("/flush/obr17q/genomeRDD-chr22").cache()



    /*
    val writer = new PrintWriter(HOME + "pgp.json", "UTF-8")
    val m2JSONArray = new JSONArray()
    dataFrame.collect().foreach(p => {
      val mJSONArray = new JSONArray(p._2.toArray)
      val mJSONObject = new JSONObject()
      mJSONObject.put("variants", mJSONArray)
      mJSONObject.put("userID", p._1)
      m2JSONArray.put(mJSONObject)
    })
    writer.println( m2JSONArray )
    writer.close()
    */
    
    

    
    
    
    val kStartTime = System.currentTimeMillis()
    val model = KMeans.train(dataFrame.map(_._3), k, 300, 1, "random")
    //model.save(sc, "myModelPath")
    //val model = KMeansModel.load(sc, "myModelPath")
    val kEndTime = System.currentTimeMillis()


    
    
    val WSSSE = model.computeCost(dataFrame.map(_._3))
    
   
    
    
    /** predictions = RDD(IndividualID, DistanceFromCenter, Centroid) **/
    val predictions = IndividualVariants.map(p => {
      (p._1, Vectors.sqdist(p._3, model.clusterCenters(model.predict(p._3))), model.predict(p._3) )
    })
     
    val SuperPopulationUniqueId = Populations.map(_.SuperPopulationId).distinct().zipWithIndex() //For ARI
    //val SuperPopulationUniqueId = Populations.map(p => (p._2, p._1)).distinct()
    
    
    // Build RDD of tuples of predictions
    val predVsExpec = predictions
    .map(p => (p._1, p._3)) // (IndividualID, Centroid)
    .join(Populations.map(_.toIndoAll)) // (IndividualID, (Centroid, (PopulationId, SuperPopulationId, something, something)))
    .map(p => (p._2._2._2, (p._1, p._2._1, p._2._2._3, p._2._2._4, p._2._2._5, p._2._2._6) )) // (PopulationName, (IndividualID, Centroid))
    .join(SuperPopulationUniqueId) // (PopulationName, ((IndividualID, Centroid), PopulationID))
    .map(p => (p._2._1._1, p._2._1._2, p._2._2, p._1, p._2._1._3,p._2._1._4,p._2._1._5,p._2._1._6)) // (IndividualID, Centroid, PopulationID, PopulationName)

    

    
    
    .sortBy(_._2, true, 1)
    .collect()
    
    //predVsExpec.foreach( p => println( "%s: %s - %s, %s, [%s, %s, %s, %s]".format(p._1, p._2, p._4, p._5, p._6, p._7, p._8, p._9) ))
    predVsExpec.foreach( p => println( "%s: %s - %s, [%s, %s, %s, %s]".format(p._1, p._2, p._4, p._5, p._6, p._7, p._8) ))

    // Find the Adjusted Rand Index.
    // Must have Python and module Scikit installed. 

    
    //val pythonPath ="/Library/Frameworks/Python.framework/Versions/2.7/bin/python"
    val clustered = "[%s]".format(predVsExpec.map(_._2.toString()).reduceLeft(_+","+_))
    val expected = "[%s]".format(predVsExpec.map(_._3.toString()).reduceLeft(_+","+_))
    val adjustedRandIndex = GetRandIndex(clustered, expected)
    
    println("Metrics:")
    //println("Pre-processing time: %s seconds".format((pEndTime - pStartTime)/1000.0))
    println("k-Means time: %s seconds".format((kEndTime - kStartTime)/1000.0))
    println("Within Set Sum of Squared Errors = %s".format(WSSSE))
    println("Adjusted Rand Index: %s".format(adjustedRandIndex))
    //println("From %s alleles".format(NoOfAlleles))
    
        
    
    
    // Save 'predictions' as a SIF file
    //val f = File("my-test.txt")
    //val siffy = predictions.map(p => (f"${p._1}\t${p._2}\t${p._3}"))
    //siffy.coalesce(1).saveAsTextFile("the.sif")

    
    //println(sqdist(clusters.clusterCenters(0),clusters.clusterCenters(1)))
    //println(sqdist(clusters.clusterCenters(0),clusters.clusterCenters(2)))
    //println(sqdist(clusters.clusterCenters(1),clusters.clusterCenters(2)))
    //println(    clusters.clusterCenters(1) )
    //val m = mat.numRows()
    //val n = mat.numCols()
    //println("Rows (m): " + m)
    //println("Cols (n): " + n)
    //val variants = rows.map(e => Row(e:_*))

    

    
  } 
}