package com.example.utils


import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author lqb
 * @date 2022/10/14 15:19
 */
object SparkUtil {
  def createSparkSession(clazz: Class[_],master:String = "local[4]",partitions:Int = 4):SparkSession={
    SparkSession.builder()
      .appName(clazz.getSimpleName.stripSuffix("$"))
      .master(master)
      .config("spark.serializer",classOf[KryoSerializer].getName)
      .config("spark.sql.shuffle.partitions",partitions)
      .getOrCreate()
  }

  def readCsvFiles(filePath:String,sparkSession: SparkSession): DataFrame ={
    sparkSession
      .read
      .option("sep","\\t")
      .option("header","true")
      .csv(filePath)
  }

  def main(args: Array[String]): Unit = {
    println(classOf[KryoSerializer].getName)
    println(this.getClass.getSimpleName)
  }

}
