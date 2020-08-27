package com.itcast.dmp.tags

import ch.hsr.geohash.GeoHash
import cn.itcast.etl.ETLRunner
import com.itcast.dmp.area.BusinessAreaRunner
import org.apache.spark.sql.{DataFrame, SparkSession}

object tagRunner {


  def main(args: Array[String]): Unit = {

    import com.itcast.dmp.utils.SparkConfigHelper._
    val spark = SparkSession.builder()
      .appName("tagrUNNER")
      .master("local[6]")
      .loadConfig()
      .getOrCreate()

    import com.itcast.dmp.utils.KuduHelper._
    val odsOption = spark.readKuduTable(ODS_TABLE)
    val areaOption = spark.readKuduTable(AREA_TABLE)

    if (odsOption.isEmpty || areaOption.isEmpty) {
      return
    }

    val ods = odsOption.get
    val area=areaOption.get

    import spark.implicits._
    import org.apache.spark.sql.functions._


    val geoHash = udf(toGeoHash _)
    val odsWithGeoHash=ods.withColumn("geoHash", geoHash('longitude,'latitude))
    val odsWithArea: DataFrame = odsWithGeoHash.join(
      area,
      odsWithGeoHash.col("geoHash") === area.col("geoHash"),
      "left"
    )


  }

  def toGeoHash(longitude: Double, latitude: Double) = {
    GeoHash.withCharacterPrecision(latitude, longitude, 8).toBase32
  }

  private val ODS_TABLE = ETLRunner.ODS_TABLE_NAME
  private val AREA_TABLE = BusinessAreaRunner.AREA_TABLE_NAME

}
