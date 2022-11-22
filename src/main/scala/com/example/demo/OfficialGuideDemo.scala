package com.example.demo

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
 * @author lqb
 * @date 2022/11/15 19:20
 */
object OfficialGuideDemo extends Serializable {
  def main(args: Array[String]): Unit = {
    testRddActions()
  }

  def testRddTransformations(): Unit = {
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
    //参数说明：zeroValue=初始值 sepOp=分区内的操作  comOp=分区之间的操作
    //举例： 输入数据
    // Array((hadoop,1), (flink,1), (yarn,1), (flink,1), (spark,1), (flink,1), (hadoop,1))
    // Array((hadoop,1), (flink,1), (yarn,1), (flink,1), (spark,1), (flink,1), (hadoop,1))
    // 每个分区内hadoop seqOp (0,1) => (1,2) 取2 => (2,1) => (3,2) 取3 => 最终分区结果（hadoop,3） comOp (hadoop,3 + 3)
    val wordTuple: RDD[(String, Int)] = wordRdd.map((_, 1))
    wordTuple.glom().collect().foreach(i=> println(i.mkString("Array(", ", ", ")")))
    val aggregateWordCount: RDD[(String, Int)] = wordTuple.aggregateByKey(0)((i, j) => Math.max(i + 1, j + 1), _ + _)
    aggregateWordCount.glom().collect().foreach(i => println(i.mkString("Array(", ", ", ")")))
    aggregateWordCount.foreach(println)
    //sortByKey
    val sortWordCountRdd: RDD[(String, Int)] = reduceWordCount.sortByKey(ascending = false,2)
    println("sortWordCountRdd==" + sortWordCountRdd.collect().toList)
    //join
    val joinWordCountRdd: RDD[(String, (Int, Int))] = reduceWordCount.join(reduceWordCount)
    println("joinWordCountRdd==" + joinWordCountRdd.collect().toList)
    //cogroup
    val intRdd: RDD[(String, Int)] = sparkContext.parallelize(Seq(("hadoop", 1), ("flink", 1)))
    val strRdd: RDD[(String, String)] = sparkContext.parallelize(Seq(("hadoop", "hadoop"), ("flink", "flink")))
    val coGroupRdd: RDD[(String, (Iterable[Int], Iterable[String]))] = intRdd.cogroup(strRdd)
    coGroupRdd.foreach(i => {
      val tuple: (Iterable[Int], Iterable[String]) = i._2
      val intIter: Iterable[Int] = tuple._1
      println("intIter==" + intIter.toList)
      val strIter: Iterable[String] = tuple._2
      println("strIter==" + strIter.toList)
    })
    //cartesian
    val cartesianRdd: RDD[((String, Int), (String, String))] = intRdd.cartesian(strRdd)
    println("cartesianRdd===" + cartesianRdd.take(100).toList)
    //coalesce
    val coalesceRdd1: RDD[(String, Int)] = reduceWordCount.coalesce(3)
    val coalesceRdd2: RDD[(String, Int)] = reduceWordCount.coalesce(3, shuffle = true)
    println("coalesceRdd1==")
    coalesceRdd1.glom().collect().toList.foreach(i => println(i.mkString("Array(", ", ", ")")))
    println("coalesceRdd2==")
    coalesceRdd2.glom().collect().toList.foreach(i => println(i.mkString("Array(", ", ", ")")))
    //repartition
    val repartitionRdd: RDD[(String, Int)] = reduceWordCount.repartition(3)
    repartitionRdd.glom().collect().toList.foreach(i=>println(i.mkString("Array(", ", ", ")")))
    //repartitionAndSortWithinPartitions
    val repartitionAndSortWithinPartitionsRdd: RDD[(String, Int)] = reduceWordCount.repartitionAndSortWithinPartitions(new MyPartitioner(3))
    repartitionAndSortWithinPartitionsRdd.glom().collect().toList.foreach(i=>println(i.mkString("Array(", ", ", ")")))
  }

  def testRddActions():Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("testRddActions")
    val sparkContext: SparkContext = new SparkContext(conf)
    val wordCountData: RDD[String] = sparkContext.textFile("src/main/resources/datas/word.txt")
    val wordRdd: RDD[String] = wordCountData.flatMap(i => i.split(" "))
    val wordCountRdd: RDD[(String, Int)] = wordRdd.map((_, 1))
    //reduce
    val reduceResult: (String, Int) = wordCountRdd.reduce((i, j) => (j._1, i._2 + j._2))
    println("reduceResult===" + reduceResult)
    //count
    val count: Long = wordCountRdd.count()
    println("count==" + count)
    //first
    val first: (String, Int) = wordCountRdd.first()
    println("first==" + first)
    //take
    val takeRdd: Array[(String, Int)] = wordCountRdd.take(3)
    println("takeRdd==" + takeRdd.mkString("Array(", ", ", ")"))
    //takeSample
    val takeSampleRdd: Array[(String, Int)] = wordCountRdd.takeSample(withReplacement = true, 5)
    println("takeSampleRdd==" + takeSampleRdd.mkString("Array(", ", ", ")"))
    //takeOrdered
    val takeOrderedRdd: Array[String] = wordCountData.takeOrdered(1)
    println("takeOrderedRdd==" + takeOrderedRdd.mkString("Array(", ", ", ")"))
    //saveAsTextFile
    //wordCountData.saveAsTextFile("src/main/resources/datas/out.txt")
    //saveAsSequenceFile
    //wordCountRdd.saveAsSequenceFile("src/main/resources/datas/out1")
    //countByKey
    val countByKeyMap: collection.Map[String, Long] = wordCountRdd.countByKey()
    println("countByKeyMap" + countByKeyMap)
    //cause shuffle operations: repartition and coalesce, ‘ByKey operations (except for counting) like groupByKey and reduceByKey, and join operations like cogroup and join.
    wordCountRdd.persist()
    wordCountRdd.cache()
    /*
        rdd persist()
        storage-level:MEMORY_ONLY  MEMORY_AND_DISK   MEMORY_ONLY_SER(Java and Scala)  MEMORY_AND_DISK_SER(Java and
        Scala)  DISK_ONLY  MEMORY_ONLY_2, MEMORY_AND_DISK_2, etc. OFF_HEAP (experimental)
        如何选择storage-level
        1、如果MEMORY_ONLY合适 优先MEMORY_ONLY
        2、如果1不行 再尝试MEMORY_ONLY_SER 并且选择快的序列化框架比如Kryo
        3、除非你要计算的dataset非常大 万不得已不要溢写到disk
        4、如果想快速恢复，可以选择replicated ones，如MEMORY_ONLY_2, MEMORY_AND_DISK_2

        spark采用lru自动干掉cache 如果想手动改的话可以使用RDD.unpersist()
    */
    //broadcast 释放使用unpersist 后续再使用会重新广播  永久释放请选择destroy
    val bcValue: Broadcast[ArrayBuffer[String]] = sparkContext.broadcast(ArrayBuffer("hadoop", "flink"))
    println(bcValue.value)
    bcValue.unpersist()
    bcValue.destroy()
    //accumulator
    val accumulator: LongAccumulator = sparkContext.longAccumulator("my ac")
    val intRdd: RDD[Int] = sparkContext.parallelize(Seq(1, 2, 3, 4, 5))
    intRdd.foreach(accumulator.add(_))
    println("accumulator==" + accumulator)
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

  class MyPartitioner(partitions:Int) extends Partitioner{
    require(partitions >= 0,"partitions cannot be negative")
    override def numPartitions: Int = partitions

    override def getPartition(key: Any): Int = {
      val res: String = key.asInstanceOf[String]
      Math.abs(res.hashCode)%partitions
    }
  }
}
