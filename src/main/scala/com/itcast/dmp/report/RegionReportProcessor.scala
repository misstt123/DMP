package com.itcast.dmp.report

import cn.itcast.etl.ETLRunner
import com.itcast.dmp.utils.KuduHelper
import org.apache.kudu.{Schema, Type}
import org.apache.kudu.ColumnSchema.ColumnSchemaBuilder
import org.apache.kudu.Type
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 统计数据集的地域分布报表
 */
object RegionReportProcessor {


  def main(args: Array[String]): Unit = {
    import com.itcast.dmp.utils.KuduHelper._
    import com.itcast.dmp.utils.SparkConfigHelper._
    val spark = SparkSession.builder()
      .appName("regionReport")
      .master("local[6]")
      .loadConfig()
      .getOrCreate()

    val sourceDF_option: Option[DataFrame] = spark.readKuduTable(this.SOURCE_TABLE_NAME)
    if (sourceDF_option.isEmpty) {
      return
    }
    import spark.implicits._
    import org.apache.spark.sql.functions._
    val sourceDF: DataFrame = sourceDF_option.get
    val result = sourceDF.groupBy($"region", $"city")
      .agg(count("*") as "count")
      .select('region, 'city, 'count)

    import scala.collection.JavaConverters._
    spark.createKuduTable(TARGET_TABLE_NAME, schema, keys)
    result.saveToKudu(TARGET_TABLE_NAME)


  }


  private val keys = List("region", "city")
  private val SOURCE_TABLE_NAME = ETLRunner.ODS_TABLE_NAME
  private val TARGET_TABLE_NAME = "report_data_region_" + KuduHelper.formattedDate()
  import scala.collection.JavaConverters._
  private val schema = new Schema(
    List(
      new ColumnSchemaBuilder("region", Type.STRING).nullable(false).key(true).build(),
      new ColumnSchemaBuilder("city", Type.STRING).nullable(false).key(true).build(),
      new ColumnSchemaBuilder("count", Type.INT64).nullable(false).key(false).build()
    ).asJava
  )
}
