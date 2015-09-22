package au.csiro.obr17q.variantspark

import au.csiro.obr17q.variantspark.CommonFunctions._
import au.csiro.obr17q.variantspark.CustomSettings._

import au.com.bytecode.opencsv.CSVParser
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.{Vector=>MLVector, Vectors}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.rdd.RDD
import sys.process._
import java.util.Arrays;
import java.io.Writer;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import org.json.JSONObject;
import org.json.JSONArray;
import scala.io.Source





object VcfClustering extends SparkApp {
  conf.setAppName("VCF cluster")
  def main(args:Array[String]) {

    val HOME = if (masterUrl == "local") "/Users/obr17q/" else "/OSM/HOME-CDC/obr17q/"
    val args1 = {
      if (masterUrl == "local") {
        /*
         * Define offline variables here!!!
         */
        Array(
            "data/merged.vcf", //Input VCF file
            "0", "2811814",  //Filter for variants between these two values
            "3",             //Number of clusters (k)
            "",   //Groups for inclusion
            "",   //Groups for exclusion
            "0.1"            //Sample size (0 - 1)
        )
      } else {
      args
      }
    }
    if (args1.length < 1) {
        println("Usage: CsvClusterer <input-path>")
    }


    val StartPos = args1(1).toLong
    val EndPos = args1(2).toLong
    val k = args1(3).toInt
    val IncludeGroups = args1(4).split('|')
    val ExcludeGroups = args1(5).split('|')
    val SampleSize = args1(6).toFloat
    val VariantRe = """^[A-Za-z]{2}[A-Fa-f0-9]{4,6}$""".r // More generic individual ID matcher
    
    // The VCF file(s)
    val VcfFiles = sc.textFile(args1(0), 10)
        
    // Returns the heading row from the FASTA file
    val Headings = VcfFiles
    .filter( _.startsWith("#CHROM") )
    .map(
      line => {
        val parser = new CSVParser('\t') 
        parser.parseLine(line)
    } ).map(e => List(e:_*)).first()
        

    val PopFiles = Source.fromFile("data/PGPParticipantSurvey-20150831064509.csv").getLines()
    val Populations = sc.parallelize(new PopulationMap(PopFiles, "Participant", ',', 0, 16 ).returnMap(IncludeGroups, ExcludeGroups).toList)

    //val PopFiles = Source.fromFile("data/ALL.panel").getLines()
    //val Populations = sc.parallelize(new PopulationMap(PopFiles, "sample", '\t', 0, 1 ).returnMap(IncludeGroups, ExcludeGroups).toList)
    
    Populations.foreach { p => println(p.IndividualId) }
   
    // Array of elements for each line in the VCF file
    // Array elements are zipped with the VCF heading
    val AllVariants = VcfFiles
    .filter( !_.startsWith("#") )
    .mapPartitions( lines => {
      val parser = new CSVParser('\t')
      lines.map( line => {
        parser.parseLine(line)
        .zip(Headings)
      } )
      //.filter(g => (g(0)._1 == "2") ) //Filter for specified chromosome
      //.filter(g => (g(1)._1.toInt >= StartPos && g(1)._1.toInt <= EndPos) ) //Filter for variant position
    } ).sample(false, SampleSize)

    val pStartTime = System.currentTimeMillis();
    // The total number of alleles present in the VCF file.
    val NoOfAlleles = AllVariants.count().toInt

    
    
    
    
    
    // Vector of elements for each individual
    // Vector elements are zipped with the Individual ID
    val SparseVariants = AllVariants
    .zipWithIndex // zip each row array (of alleles) with a unique index
    .flatMap( (h) => {
      h._1
      .filter( v => VariantRe.findFirstIn(v._2).isDefined ) //filter kv pairs that aren't variants (don't match regex)
      .map( i => ( i._2, (h._2.toInt, variantDist(i._1, 0)) ) ) //convert strings to distance
      .filter(_._2._2 != 0) //remove 0-variants to make data sparce
    })    
    .groupByKey //group by individual ID, i.e. get RDD of individuals
    .join(Populations.map(_.toIndo)) //filter out population groups you don't want
    .map(h => (h._1, Vectors.sparse(NoOfAlleles, h._2._1.to[Seq] ) ))//.cache() //create sparse vectors





    /** Print populations included and number of individuals **/
    //SparseVariants.map(p => (p._1, 1)).join(Populations).map(p => (p._2._2._1, 1)).reduceByKey(_ + _).collect().foreach(println)

    /** Print the count of variants for each individual **/
    //val countt = SparseVariants.map(p => (p._2.toArray).reduce(_ + _)).collect().foreach(println)


    //Populations.collect().foreach(println)
    println("Processed VCF file with %s alleles.".format( NoOfAlleles))
    val pEndTime = System.currentTimeMillis();  






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


     val dataFrame = SparseVariants.map(p => (p._2) )


    //dataFrame.saveAsObjectFile("/flush/obr17q/genomeRDD-PGP")


    //val dataFrame: RDD[(String, Array[Double])] = sc.objectFile("/flush/obr17q/genomeRDD-PGP").cache();
    //val dataFrame: RDD[Vector] = sc.objectFile("/flush/obr17q/genomeRDD-chr22").cache();




    /*
    val writer = new PrintWriter(HOME + "pgp.json", "UTF-8");
    val m2JSONArray = new JSONArray()
    dataFrame.collect().foreach(p => {
      val mJSONArray = new JSONArray(p._2.toArray)
      val mJSONObject = new JSONObject()
      mJSONObject.put("variants", mJSONArray)
      mJSONObject.put("userID", p._1)
      m2JSONArray.put(mJSONObject)
    })
    writer.println( m2JSONArray )
    writer.close();
    */
    
    

    
    
    
    val kStartTime = System.currentTimeMillis();
    val KMeansModel = KMeans.train(dataFrame, k, 300)
    val kEndTime = System.currentTimeMillis();

    val WSSSE = KMeansModel.computeCost(dataFrame)
    
   
    
    
    /** predictions = RDD(IndividualID, DistanceFromCenter, Centroid) **/
    val predictions = SparseVariants.map(p => {
      (p._1, Vectors.sqdist(p._2, KMeansModel.clusterCenters(KMeansModel.predict(p._2))), KMeansModel.predict(p._2) )
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
    val pythonPath = "/usr/bin/python"
    //val pythonPath ="/Library/Frameworks/Python.framework/Versions/2.7/bin/python"
    val clustered = "[%s]".format(predVsExpec.map(_._2.toString()).reduceLeft(_+","+_))
    val expected = "[%s]".format(predVsExpec.map(_._3.toString()).reduceLeft(_+","+_))

    val pythonFunc = "from sklearn.metrics.cluster import adjusted_rand_score;print adjusted_rand_score(%s, %s)".format(clustered, expected)
    val adjustedRandIndex = Seq(pythonPath, "-c", pythonFunc) !!
    
    println("Metrics:")
    println("Pre-processing time: %s seconds".format((pEndTime - pStartTime)/1000.0));
    println("k-Means time: %s seconds".format((kEndTime - kStartTime)/1000.0));
    println("Within Set Sum of Squared Errors = %s".format(WSSSE))
    println("Adjusted Rand Index: %s".format(adjustedRandIndex))
    println("From %s alleles".format(NoOfAlleles))
    
        
    
    
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