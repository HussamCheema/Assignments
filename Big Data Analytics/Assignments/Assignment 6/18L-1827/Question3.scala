//Question # 3

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


object Question3 {
  
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
                        
      val df2 = df.rdd.map(r => (r(0).toString().toDouble,r(5).toString().toDouble,r(9).toString().toDouble,r(11).toString().toDouble,r(12).toString().toDouble,r(13).toString().toDouble,r(14).toString().toDouble)).cache()
      //rows.foreach(println)
      val DataVectors = df2.map(r => Vectors.dense(r._1, r._2,r._3, r._4,r._5, r._6,r._7))
      
		  val kMeansModel = KMeans.train(DataVectors, 7, 70)
		  
		  val predictions = df2.map{r => (r._1, kMeansModel.predict(Vectors.dense(r._1, r._2,r._3, r._4,r._5, r._6,r._7)))}
      
      val df1 = predictions.toDF("id", "cluster")
      
      kMeansModel.clusterCenters.foreach(println)
            
    
    }
  
}