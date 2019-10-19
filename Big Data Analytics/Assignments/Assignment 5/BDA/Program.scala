//Submitted By Hussam-Ul-Hussain (18L-1827)
package bigdata

import org.apache.spark.SparkContext;
import org.apache.spark.SparkConf;
import org.apache.spark.Partitioner
import org.apache.spark.HashPartitioner

class MyPartitioner(override val numPartitions: Int) extends Partitioner {

	override def getPartition(key: Any): Int = key match {
		case s: String => {
			key.hashCode()%numPartitions
		  }   
	  }
}


object WordCount {
  
  def main(args:Array[String]){

      val s = "C:/winutils";
      System.setProperty("hadoop.home.dir", s);
    
      if (args.length !=3 ) throw new Exception( "Enter the input and output file name ")
      val infile = args(0)
      val outfile1 = args(1)
      val outfile2 = args(2)
      //val outfile3 = args(3)
      val conf = new SparkConf().setAppName("WordCount").setMaster("local")
      val sc = new SparkContext(conf)
      val Data = sc.textFile(infile)
      
      //Splitting the data and applying the conditions
      val rdd = Data.map(x=>x.split(","))
                    .map(x=>(x(0).substring(0,2),x(0).substring(2,3),x(0),x(1),x(2),x(0).substring(3,7)))
                    .filter(x => x._1.toInt > 94 || x._1.toInt < 19).distinct()
                    

      val objects = rdd.collect
      
      //Getting unique campuses
      var campuses = objects.map(x => x._2).distinct
      
      val rdd2 = sc.parallelize(for {
        x <- 0 until objects.length
      } yield (objects(x)._2, objects(x)))
      
      val rddTwoP = rdd2.sortBy({case x if  x._2._1  >= "95" => (x._2._2,"-"+x._2._1, x._2._6)
                          				case x if  x._2._1  <= "95" => (x._2._2,x._2._1, x._2._6)
                          			}, true).partitionBy(new MyPartitioner(8))
      rddTwoP.persist()
      
      val totalStudents = rddTwoP.map(x=> (x._2._3,x._1)).distinct().map(x=> (x._2,1)).reduceByKey(_+_)
      
      println("-----------------Total Students by Campus------------------")
      totalStudents.foreach(println)

      val totalFailures = rddTwoP.filter(x=> x._2._5 == "F").map(x=> (x._1+x._2._4,1)).reduceByKey(_+_)
      
      println("-----------------Course Failures by Campus-----------------")
      totalFailures.foreach(println)

      val GPA_Modified = rddTwoP.map{

        case x if x._2._5 == "A" => ("Campus="+x._1,"Year="+x._2._1,"RollNo="+x._2._3,"Subject="+x._2._4,"Grade="+x._2._5)
        case x if x._2._5 == "B" => ("Campus="+x._1,"Year="+x._2._1,"RollNo="+x._2._3,"Subject="+x._2._4,"Grade="+x._2._5)
        case x if x._2._5 == "C" => ("Campus="+x._1,"Year="+x._2._1,"RollNo="+x._2._3,"Subject="+x._2._4,"Grade="+x._2._5)
        case x if x._2._5 == "D" => ("Campus="+x._1,"Year="+x._2._1,"RollNo="+x._2._3,"Subject="+x._2._4,"Grade="+x._2._5)
        case x if x._2._5 == "F" => ("Campus="+x._1,"Year="+x._2._1,"RollNo="+x._2._3,"Subject="+x._2._4,"Grade="+x._2._5)
          
      }
      
      val GPA = rddTwoP.map{

        case x if x._2._5 == "A" => (x._2._3,x._2._1,x._2._4,4)
        case x if x._2._5 == "B" => (x._2._3,x._2._1,x._2._4,3)
        case x if x._2._5 == "C" => (x._2._3,x._2._1,x._2._4,2)
        case x if x._2._5 == "D" => (x._2._3,x._2._1,x._2._4,1)
        case x if x._2._5 == "F" => (x._2._3,x._2._1,x._2._4,0)
        
      }
      
      val newData = GPA.map(x => (x._1,(x._4, 1)))
		                .reduceByKey((x,y) =>(x._1 + y._1, x._2 + y._2))
		                .map(x =>("Student " + x._1, ("GPA =" + (x._2._1.toDouble / x._2._2.toDouble))))
      
		  
		  println("-----------------Total GPA of the Students-----------------")
		  newData.foreach(println)
      
      GPA_Modified.saveAsTextFile(outfile1)
      totalStudents.saveAsTextFile(outfile2)
      //totalFailures.saveAsTextFile(outfile3)
      
      
      sc.stop
    
    }
}