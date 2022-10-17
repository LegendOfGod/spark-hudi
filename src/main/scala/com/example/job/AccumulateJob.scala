package com.example.job

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author lqb
 * @date 2022/10/13 15:52
 */
object AccumulateJob {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("ClosureJob")
    val sparkContext: SparkContext = new SparkContext(sparkConf)
    var count: Int = 0;
    val count1: LongAccumulator = sparkContext.longAccumulator("count")
    val rdd: RDD[Int] = sparkContext.parallelize(List(1, 2, 3, 4))
    rdd.foreach(count1.add(_))
    println(count1)
  }
}
