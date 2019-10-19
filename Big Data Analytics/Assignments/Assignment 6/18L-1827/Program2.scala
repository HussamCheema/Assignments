//Submitted By Hussam-Ul-Hussain (18L-1827)
package bigdata2

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import shapeless.ops.nat.ToInt
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
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.clustering._


object Program2 {
  
  def main(args:Array[String]){

      
      val s = "C:/winutils";
      System.setProperty("hadoop.home.dir", s);
    
      if (args.length !=3 ) throw new Exception( "Enter the input and output file name ")
      val infile = args(0)

      val spark = SparkSession.builder()
                              .master("local[*]")
                              .appName("SparkDFbasic")
                              .getOrCreate()
                          
      import spark.implicits._
      
      val df = spark.read.format("csv")
                    .option("sep", ";")
                    .option("header", "true")
                    .option("inferSchema", "true")
                    .load(infile)
      
      
      val df2 = df.groupBy("job").agg(count($"job").alias("Count"),max($"balance").alias("Maximum"),min($"balance").alias("Minimum"),mean($"balance").alias("Average"))
      df2.show()
      
    	val MaritalCount = df.filter("marital == 'married' OR marital == 'single'")
		                            .groupBy("marital").agg(count($"education"))
		                            
      
      MaritalCount.show()
     
      
      spark.stop
    
    }
}