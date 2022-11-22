package com.example.demo

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author lqb
 * @date 2022/11/22 19:38
 */
object SqlDemo extends Serializable {
  def main(args: Array[String]): Unit = {
    testStarted()
  }

  def testStarted():Unit = {
    //sparkSql入口sparkSession
    val sparkSession: SparkSession = SparkSession
      .builder()
      .master("local[4]")
      .appName("testStarted")
      .getOrCreate()
    //createDataframe
    val people: DataFrame = sparkSession.read.json("src/main/resources/datas/people.json")
    people.printSchema()
    people.show()
    //select
    import sparkSession.implicits._
    people.select($"name",'age).show()
    //
  }
}
