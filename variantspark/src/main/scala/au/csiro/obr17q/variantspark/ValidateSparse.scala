package au.csiro.obr17q.variantspark

import au.csiro.obr17q.variantspark.CommonFunctions._
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors

import scala.io.Source

object ValidateSparse extends SparkApp {
  conf.setAppName("VCF cluster")
  
  
  def assert(test:Boolean, msg: String) {
    if (!test) {
      throw new RuntimeException("Failed "+ msg)
    }   
  }
  def main(args:Array[String]) {

   
    if (args.length < 1) {
        println("Usage: CsvClusterer <input-path>")
    }

    val inputFiles = args(0)

    
    
    
    
    //val PopFiles = Source.fromFile("data/PGPParticipantSurvey-20150831064509.csv").getLines()
    //val Populations = sc.parallelize(new PopulationMap(PopFiles, 1, ',', 0, 16 ).returnMap(IncludeGroups, ExcludeGroups))

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    val sparseVariat = sqlContext.parquetFile(inputFiles)    
    println(sparseVariat.schema)

    
    val IndividualVariants = 
      sparseVariat.rdd
        .map(r=> (r.getString(0), r.getInt(1),
          r.getSeq[Int](2).toArray, r.getSeq[Double](3).toArray))
        .foreach{ case (subjectId, size, indices, values) => 
        

          
          println(s"${subjectId}: size: ${size}, indices: ${indices.length},values: ${values.length}" + 
              s", ${indices.toSeq.min}, ${indices.toSeq.max},${values.toSeq.min}, ${values.toSeq.max}")
          assert(size > 0, "Size > 0 ")
          assert(indices.length == values.length, "indices.length == values.length")
          assert(indices.toSeq.max < size, "indices.toSeq.max < size")
          assert(indices.toSeq.min >=0 , "indices.toSeq.min >=0")
          assert(values.toSeq.min >=0 , "values.toSeq.min >=0")
    }
        
    
  } 
}