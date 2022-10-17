package com.example.job

import com.example.utils.SparkUtil
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author lqb
 * @date 2022/10/13 19:19
 */
object SparkHudiETLJob {
  val tableName: String = "ods_did"
  val basePath:String = "/datas/hudi-warehouse/ods_did"
  val originDataPath:String = "file:///E:\\project-learn\\spark-learning\\src\\main\\resources\\datas";

  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkUtil.createSparkSession(this.getClass)
    val origin: DataFrame = SparkUtil.readCsvFiles(originDataPath, sparkSession)
    val processResult:DataFrame = SparkUtil.process(origin:DataFrame)
    SparkUtil.saveToHudi(processResult,tableName,basePath)
    sparkSession.stop()
  }
}
