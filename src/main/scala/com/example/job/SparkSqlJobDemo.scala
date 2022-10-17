package com.example.job

import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}


/**
 * @author lqb
 * @date 2022/10/13 16:09
 */
object SparkSqlJobDemo {
  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder().master("local[2]").appName("SparkSqlJobDemo").getOrCreate()
    import sparkSession.implicits._
    val data: Seq[(String, String, Int)] = Seq(
      ("Thin", "Cell phone", 6000),
      ("Normal", "Tablet", 1500),
      ("Mini", "Tablet", 5500),
      ("Ultra thin", "Cell phone", 5500),
      ("Very thin", "Cell phone", 6000),
      ("Big", "Tablet", 2500),
      ("Bendable", "Cell phone", 3000),
      ("Foldable", "Cell phone", 3000),
      ("Pro", "Tablet", 4500),
      ("Pro2", "Tablet", 6500)
    )

    val source: DataFrame = data.toDF("product", "category", "revenue")

    val windowSpec: WindowSpec = Window.partitionBy('category)
      .orderBy('revenue.desc)


    source.select(
      'product, 'category, 'revenue,
      ((functions.max('revenue) over windowSpec) - 'revenue) as 'revenue_difference
    ).show()
  }
}

case class People(id: Int, name: String){
  override def toString: String = s"People($id,$name)"
}

