package com.example.demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
 * @author lqb
 * @date 2022/11/15 19:20
 */
object OfficialGuideDemo extends Serializable {
  def main(args: Array[String]): Unit = {
    testRdd()
  }

  def testRdd(): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("RDDTest")
    val sparkContext: SparkContext = new SparkContext(conf)
    val data1: RDD[String] = sparkContext.textFile("src/main/resources/datas/word.txt")
    val data2: RDD[String] = sparkContext.textFile("src/main/resources/datas/wordb.txt")
    //flatMap
    val wordRdd: RDD[String] = data1.flatMap(i => i.split(" "))
    println("wordRdd==" + wordRdd.collect().toList)
    //map
    val fistLetter: RDD[Char] = wordRdd.map(MyFunction.firstLetter)
    val list: List[Char] = fistLetter.collect.toList
    println("fistLetter==" + list)
    //persist
    data1.persist()
    //reduce
    val reduceStr: String = data1.reduce((a, b) => a + b)
    println("str1==" + reduceStr)
    val reduceStr2: String = data1.reduce((a, b) => a + b)
    println("str2==" + reduceStr2)
    //persist
    wordRdd.persist()
    //understand closure
    val count: Int = 0
    wordRdd.foreach(_ => count + 1)
    println("countValue==" + count)
    //mapPartition
    val mapPartitionData: RDD[Char] = data2.mapPartitions(i => {
      val arrayBuffer: ArrayBuffer[Char] = new ArrayBuffer
      i.foreach(i => arrayBuffer += MyFunction.firstLetter(i))
      arrayBuffer.iterator
    })
    println("mapPartition===",mapPartitionData.collect().toList)
    //intersection
    val interSectionData: RDD[String] = data1.intersection(data2)
    println("interSectionData===" + interSectionData.take(100).toList)
    //union
    val unionData: RDD[String] = data1.union(data2)
    println("unionData===" + unionData.collect().toList)
    //distinct
    val distinctData: RDD[String] = data1.distinct()
    println("distinctData===" + distinctData.take(100).toList)
    //groupByKey
    val groupByKey: RDD[(String, Iterable[Int])] = wordRdd.map((_, 1)).groupByKey(1)
    val groupWordCount: RDD[(String, Int)] = groupByKey.map(i => (i._1, i._2.sum))
    groupWordCount.foreach(println)
    //reduceByKey
    val reduceWordCount: RDD[(String, Int)] = wordRdd.map((_, 1)).reduceByKey((i, j) => i + j,1)
    reduceWordCount.foreach(println)
    //aggregateByKey
    val wordTuple: RDD[(String, Int)] = wordRdd.map((_, 1))
    wordTuple.foreach(println)
    val aggregateWordCount: RDD[(String, Int)] = wordTuple.aggregateByKey(0)((i, j) => Math.max(i + 1, j + 1), _ + _)
    aggregateWordCount.foreach(println)
  }

object MyFunction{
  def firstLetter(word: String): Char = {
    val array: Array[Char] = word.trim.toCharArray
    if (array.length > 0) {
      array(0)
    } else {
      ' '
    }

  }
}

}
