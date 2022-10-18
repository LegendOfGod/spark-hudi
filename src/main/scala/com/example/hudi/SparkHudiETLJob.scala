package com.example.hudi

import com.example.utils.SparkUtil
import org.apache.hudi.DataSourceWriteOptions.{PARTITIONPATH_FIELD, PRECOMBINE_FIELD, RECORDKEY_FIELD}
import org.apache.hudi.QuickstartUtils.getQuickstartWriteConfigs
import org.apache.hudi.config.HoodieWriteConfig.TBL_NAME
import org.apache.spark.sql.functions.{col, concat_ws, unix_timestamp}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * @author lqb
 * @date 2022/10/13 19:19
 */
object SparkHudiETLJob {
  val tableName: String = "ods_did"
  val basePath: String = "/datas/hudi-warehouse/ods_did"
  val originDataPath: String = "file:///E:\\project-learn\\spark-learning\\src\\main\\resources\\datas";

  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkUtil.createSparkSession(this.getClass)
    val origin: DataFrame = SparkUtil.readCsvFiles(originDataPath, sparkSession)
    val processResult: DataFrame = process(origin: DataFrame)
    saveToHudi(processResult, tableName, basePath)
    sparkSession.stop()
  }

  def saveToHudi(processResult: DataFrame, tableName: String, basePath: String): Unit = {
    processResult.write.format("hudi")
      .options(getQuickstartWriteConfigs)
      .option(PRECOMBINE_FIELD.key(), "ts")
      .option(RECORDKEY_FIELD.key(), "order_id")
      .option(PARTITIONPATH_FIELD.key(), "partitionpath")
      .option(TBL_NAME.key(), tableName)
      .mode(SaveMode.Append)
      .save(basePath)
  }


  def process(data: DataFrame): DataFrame = {
    data
      .withColumn("partitionpath",
        concat_ws("/", col("year"), col("month"), col("day")))
      .drop("year", "month", "day")
      .withColumn("ts",
        unix_timestamp(col("departure_time"), "yyyy-MM-dd HH:mm:ss")
      )
  }
}
