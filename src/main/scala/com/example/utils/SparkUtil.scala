package com.example.utils


import org.apache.hudi.DataSourceWriteOptions.{PARTITIONPATH_FIELD, PRECOMBINE_FIELD, RECORDKEY_FIELD}
import org.apache.hudi.QuickstartUtils.getQuickstartWriteConfigs
import org.apache.hudi.config.HoodieWriteConfig._
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.functions.{col, concat_ws, unix_timestamp}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * @author lqb
 * @date 2022/10/14 15:19
 */
object SparkUtil {
  def saveToHudi(processResult: DataFrame, tableName: String, basePath: String): Unit = {
    processResult.write.format("hudi")
      .options(getQuickstartWriteConfigs)
      .option(PRECOMBINE_FIELD.key(), "ts")
      .option(RECORDKEY_FIELD.key(), "order_id")
      .option(PARTITIONPATH_FIELD.key(), "partitionpath")
      .option(TBL_NAME.key(), tableName)
      .mode(SaveMode.Overwrite)
      .save(basePath)
  }


  def process(data: DataFrame): DataFrame = {
    data
      .withColumn("partitionpath",
      concat_ws("/",col("year"),col("month"),col("day")))
      .drop("year","month","day")
      .withColumn("ts",
        unix_timestamp(col("departure_time"),"yyyy-MM-dd HH:mm:ss")
      )
  }


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
      .option("inferSchema","true")
      .csv(filePath)
  }

  def main(args: Array[String]): Unit = {
    println(classOf[KryoSerializer].getName)
    println(this.getClass.getSimpleName)
  }

}
