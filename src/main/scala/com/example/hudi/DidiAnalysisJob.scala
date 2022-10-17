package com.example.hudi

import com.example.utils.SparkUtil
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author lqb
 * @date 2022/10/17 17:57
 */
object DidiAnalysisJob {
  val basePath: String = "/datas/hudi-warehouse/ods_did"

  def readDataFromHudi(basePath: String, sparkSession: SparkSession): DataFrame = {
    val ds: DataFrame = sparkSession.read.format("hudi").load(basePath)
    ds.select("order_id","product_id","type","traffic_type","pre_total_fee","start_dest_distance","departure_time")
  }

  def reportProduct(data: DataFrame): Unit = {
    val report: DataFrame = data.groupBy("product_id").count()
    val toName: UserDefinedFunction = udf(
      (productId: Int) => {
        productId match {
          case 1 => "滴滴专车"
          case 2 => "滴滴企业专车"
          case 3 => "滴滴快车"
          case 4 => "滴滴企业快车"
        }
      }
    )
    val result: DataFrame = report.select(toName(col("product_id")).as("order_type"), col("count").as("total"))
    result.show(10,truncate = false)
  }

  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkUtil.createSparkSession(this.getClass)
    val data:DataFrame = readDataFromHudi(basePath,sparkSession)
    //统计每种类型产品的订单数
    reportProduct(data)
  }
}
