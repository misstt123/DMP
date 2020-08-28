package com.itcast.dmp.tags

import ch.hsr.geohash.GeoHash
import cn.itcast.etl.ETLRunner
import com.itcast.dmp.area.BusinessAreaRunner
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable

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
    val area = areaOption.get

    import spark.implicits._
    import org.apache.spark.sql.functions._


    val geoHash = udf(toGeoHash _)
    val odsWithGeoHash = ods.withColumn("geoHash", geoHash('longitude, 'latitude))
    val odsWithArea: DataFrame = odsWithGeoHash.join(
      area,
      odsWithGeoHash.col("geoHash") === area.col("geoHash"),
      "left"
    )
    odsWithArea.map(createTags)

  }

  def createTags(row: Row):IdsWithTags = {
    // 3.4 生成标签数据
    val tags = mutable.Map[String, Int]()
    // 3.4.1 广告标识
    tags += ("AD" + row.getAs[Long]("adspacetype") -> 1)
    // 3.4.2 渠道信息
    tags += ("CH" + row.getAs[String]("channelid") -> 1)
    // 3.4.3 关键词 keywords -> 帅哥,有钱
    row.getAs[String]("keywords").split(",")
      .map("KW" + _ -> 1)
      .foreach(tags += _)
    // 3.4.4 省市标签
    tags += ("PN" + row.getAs[String]("region") -> 1)
    tags += ("CN" + row.getAs[String]("city") -> 1)
    // 3.4.5 性别标签
    tags += ("GD" + row.getAs[String]("sex") -> 1)
    // 3.4.6 年龄标签
    tags += ("AG" + row.getAs[String]("age") -> 1)
    // 3.4.7 商圈标签, 正常情况下, 在这里需要生成 GeoHash, 然后再查询 Kudu 表, 获取数据
    // 优化写法, 直接先进行 join, 把商圈信息添加进来, 然后直接取
    row.getAs[String]("area").split(",")
      .map("A" + _ -> 1)
      .foreach(tags += _)
  }

  def toGeoHash(longitude: Double, latitude: Double) = {
    GeoHash.withCharacterPrecision(latitude, longitude, 8).toBase32
  }

  private val ODS_TABLE = ETLRunner.ODS_TABLE_NAME
  private val AREA_TABLE = BusinessAreaRunner.AREA_TABLE_NAME

}
case class IdsWithTags(mainId: String, ids: Map[String, String], tags: Map[String, Int])