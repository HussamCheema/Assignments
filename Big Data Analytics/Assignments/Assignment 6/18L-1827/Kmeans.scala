//Question # 2
// Submitted by Hussam-Ul-Hussain (18L-1827)

package bigdata2

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import shapeless.ops.nat.ToInt
import org.apache.spark.ml.feature.{OneHotEncoderEstimator, StringIndexer}
import org.apache.spark.Partitioner
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec._
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import scala.concurrent.Future
import org.apache.spark.sql.{SparkSession, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import scala.concurrent.ExecutionContext.Implicits.global
import breeze.linalg.{squaredDistance, DenseVector, Vector}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.clustering._
import scala.collection.mutable.ListBuffer

object Kmeans {
  
    def parseVector(line: String): Vector[Double] = {
      
      DenseVector(line.split(',').map(_.toDouble))
      
    }
  
    def closestPoint(p:Vector[Double],centers:Array[Vector[Double]]): Int = {
      var bestIndex = 0
      
      var closest = Double.PositiveInfinity
      for (i <- 0 until centers.length) {
        val tempDist = squaredDistance(p, centers(i))
        if (tempDist < closest) {
          closest = tempDist
          bestIndex = i
        }
      }
      
      bestIndex
    }
  
  def main(args: Array[String]){

    if (args.length !=3 ) throw new Exception( "Enter the input and output file name ")
    
    val infile = args(0)
    
    val K = 7
    
    val convergeDist = 120
    
    //val outfile = args(3)
    
    val spark = SparkSession.builder()
                            .master("local[*]")
                            .appName("SparkKMeans")
                            .getOrCreate()
    
    import spark.implicits._
                            
    val df = spark.read.format("csv")
                           .option("sep", ";")
                           .option("inferSchema", "true")
                           .option("header", "true")
                           .load(infile)
    
    val df2 = df.select("age","job","marital","education","balance")
    
    // Using StringIndexer for one hot encoding of Job
    val JobEncoded = new StringIndexer()
          .setInputCol("job")
          .setOutputCol("JobEncoded")
          .fit(df2)
    
    val FirstEncoding = JobEncoded.transform(df2)
        
    // Using StringIndexer for one hot encoding of marital
    val MaritalEncoded = new StringIndexer()
          .setInputCol("marital")
          .setOutputCol("MaritalEncoded")
          .fit(df2)
    
    val SecondEncoding = MaritalEncoded.transform(FirstEncoding)
        
    // Using StringIndexer for one hot encoding of EducationEncoded
    val EducationEncoded = new StringIndexer()
                   .setInputCol("education")
                   .setOutputCol("EducationEncoded")
                   .fit(df2)
          
    val FinalEncoding = EducationEncoded.transform(SecondEncoding)
    
    val rowRDD = FinalEncoding.select("age","JobEncoded","MaritalEncoded","EducationEncoded","balance").rdd
    
    val df3 = rowRDD.map(_.mkString(","))
     
    val newData = df3.map(x => parseVector(x)).cache()
  
    val kPoints = newData.takeSample(withReplacement = false, K.toInt)
    
    //var Deltas = new ListBuffer[String]()
    
    var tempDist = Double.PositiveInfinity
    
    while(tempDist > convergeDist.toInt) {
      
      val closest = newData.map (p => (closestPoint(p, kPoints), (p, 1)))
      
      val pointStats = closest.reduceByKey{case ((p1, c1), (p2, c2))=> (p1 + p2, c1 + c2)}
      
      val newPoints = pointStats.map {pair =>
                                (pair._1, pair._2._1 * (1.0 / pair._2._2))}.collectAsMap()
      
      tempDist = 0.0
      
      for (i <- 0 until K.toInt) {
        tempDist += squaredDistance(kPoints(i), newPoints(i))
      }
      
      for (newP <- newPoints) {
        kPoints(newP._1) = newP._2
      }
      
      //Deltas.addString(new StringBuilder(tempDist.toString()))
      println("Finished iteration (delta = " + tempDist + ")")
      
      println("-------------------Clusters-------------------")
      
      kPoints.foreach(println)
      
    }
  
    //Deltas.saveAsTextFile(outfile)
    
  }
}