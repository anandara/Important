package Sparkworkouts

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object workout1 {
  
  def main(args:Array[String])
  {
    val conf = new SparkConf().setAppName("Work1").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val rdd = sc.textFile("D:/sparkdata/userinfo.txt")
    rdd.foreach(println)
    //println("Welcome to Scala...")
  }
}