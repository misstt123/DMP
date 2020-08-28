package com.itcast.dmp.area

import ch.hsr.geohash.GeoHash
import cn.itcast.etl.ETLRunner
import com.itcast.dmp.utils.HttpUtils
import org.apache.kudu.ColumnSchema.ColumnSchemaBuilder
import org.apache.kudu.{Schema, Type}
import org.apache.spark.sql.{DataFrame, SparkSession}

object BusinessAreaRunner {


  def main(args: Array[String]): Unit = {
    import com.itcast.dmp.utils.SparkConfigHelper._
    val spark = SparkSession.builder()
      .master("local[6]")
      .appName("businessArea")
      .loadConfig()
      .getOrCreate()

    import com.itcast.dmp.utils.KuduHelper._
    val odsOption: Option[DataFrame] = spark.readKuduTable(ODS_TABLE_NAME)
    val AreaOption: Option[DataFrame] = spark.readKuduTable(AREA_TABLE_NAME)

    spark.udf.register("geoHash", locationToGeoHash _)
    spark.udf.register("fetchArea", fetchArea _)


    if (odsOption.isDefined & AreaOption.isEmpty) {
      //全量打标签
      val ods = odsOption.get
      import spark.implicits._
      ods.select('longitude, 'latitude)
        .selectExpr("geoHASH(longitude,latitude) as geoHash")
        .selectExpr("fetchArea(longitude,latitude) as area")
    }
    if (odsOption.isDefined & AreaOption.isDefined) {
      val ods = odsOption.get
      val area = AreaOption.get

      import org.apache.spark.sql.functions._
      //增量打标签
      val odsWithGeoHash = ods
        .selectExpr("geoHASH(longitude,latitude) as geoHash", "longitude", "latitude")
        .withColumn("area", lit(null))

      val result = odsWithGeoHash.join(area, ods.col("geoHash") === area.col("geoHash"), "left")
        .where(area.col("area") isNull)
        .select(odsWithGeoHash.col("geoHash"), expr("fetchArea(longitude,latidude) as area"))
      spark.createKuduTable(AREA_TABLE_NAME, schema, keys)
      result.saveToKudu(AREA_TABLE_NAME)
    }
  }

  import scala.collection.JavaConverters._

  private val keys = List("geoHash")
  private val schema = new Schema(
    List(
      new ColumnSchemaBuilder("geoHash", Type.STRING).nullable(false).key(true).build(),
      new ColumnSchemaBuilder("area", Type.STRING).nullable(true).key(true).build()).asJava
  )


  def fetchArea(longitude: Double, latitude: Double) = {
    val jsonString = HttpUtils.getLocationInfo(longitude, latitude)
    jsonString.map(item => HttpUtils.parseJosn(item))
      .map(item => {
        val areaList = item.regeocode.get.addressComponent.get.businessAreas.getOrElse(List())
        areaList.map(_.name).mkString(",")
      }).getOrElse("")

  }


  def locationToGeoHash(longtiude: Double, latitude: Double) = {
    GeoHash.withCharacterPrecision(latitude, longtiude, 8).toBase32

  }

  private val ODS_TABLE_NAME = ETLRunner.ODS_TABLE_NAME
  val AREA_TABLE_NAME = "BUSINESS_AREA"

}
