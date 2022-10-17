package com.example.job

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author lqb
 * @date 2022/10/13 9:58
 */
object WorldCountJob {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("WorldCountJob")
    val sparkContext: SparkContext = new SparkContext(sparkConf)
    val rdd: RDD[String] = sparkContext.textFile("src/main/resources/file/wordCount.txt")
    val result: RDD[(String, Int)] = rdd
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey((x, y) => x + y).sortBy(_._2,ascending = true)
    println(result.collect().mkString("Array(", ", ", ")"))
  }


}
