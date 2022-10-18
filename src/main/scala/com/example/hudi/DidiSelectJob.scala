package com.example.hudi

import com.example.utils.SparkUtil
import org.apache.hudi.DataSourceReadOptions.{BEGIN_INSTANTTIME, END_INSTANTTIME, QUERY_TYPE, QUERY_TYPE_INCREMENTAL_OPT_VAL}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author lqb
 * @date 2022/10/18 9:04
 * 17592880231474
 * oldcommitTime:20221018105936
 * newcommitTime:20221018110949
 */
object DidiSelectJob {
  val basePath: String = "/datas/hudi-warehouse/ods_did"

  //basic query-默认最新快照数据snapshot query
  def readDataFromHudi(basePath: String, sparkSession: SparkSession): DataFrame = {
    val ds: DataFrame = sparkSession.read.format("hudi").load(basePath)
    ds.printSchema()
    ds.where(col("order_id").equalTo("17592880231474")).show()
    ds.show(10,truncate = false)
    ds
  }

  //time travel query- 获取这个commit时间之前的最新快照数据
  def timeTravelQuery(basePath:String,sparkSession: SparkSession):Unit={
    val ds: DataFrame = sparkSession.read.format("hudi").option("as.of.instant", "20221018110949").load(basePath)
    ds.show()
    ds.where(col("order_id").equalTo("17592880231474")).show()
  }

  //incremental query 增量快照查询  可以时间范围内的增量数据(时间范围)
  // BEGIN_INSTANTTIME 必须指定000代表所有commit END_INSTANTTIME可指定可不指定
  def incrementQuery(basePath:String,sparkSession: SparkSession):Unit={
    val beginTime:String = "20221018110949"
    val tripsIncrementalDF: DataFrame = sparkSession.read.format("hudi").
      option(QUERY_TYPE.key(), QUERY_TYPE_INCREMENTAL_OPT_VAL).
      option(BEGIN_INSTANTTIME.key(),"000").
      option(END_INSTANTTIME.key(), beginTime).
      load(basePath)
    tripsIncrementalDF.where(col("order_id").equalTo("17592880231474")).show()
  }


  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkUtil.createSparkSession(this.getClass)
//    val data:DataFrame = readDataFromHudi(basePath,sparkSession)
//    timeTravelQuery(basePath, sparkSession)
      incrementQuery(basePath, sparkSession)
  }


}
