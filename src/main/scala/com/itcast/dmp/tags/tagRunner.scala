package com.itcast.dmp.tags

import ch.hsr.geohash.GeoHash
import cn.itcast.etl.ETLRunner
import com.itcast.dmp.area.BusinessAreaRunner
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.graphframes.GraphFrame

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
    //生成标签数据
    val idsAndTags: Dataset[IdsWithTags] = odsWithArea.map(createTags(_))
    //图计算
    val vertx = idsAndTags.map(item => Vertx(item.mainId, item.ids, item.tags))
    //定要注意边的生成, 是一对多的, 是一个笛卡尔积
    val edge = idsAndTags.flatMap(item => {
      val ids = item.ids
      val result = for (id <- ids; otherId <- ids if id != otherId) yield Edge(id._2, otherId._2)
      result
    })

    val component: Dataset[VertxComponent] = GraphFrame(vertx.toDF(), edge.toDF()).connectedComponents.run().as[VertxComponent]

    //聚合
    val agg: Dataset[(String, VertxComponent)] = component.groupByKey(component => component.component)
      .reduceGroups(reduceVertex _)
    //转换
    val result = agg.map(mapTags _)
result.show()
  }

  def mapTags(vertexComponent: (String, VertxComponent)) = {
    val mainId = getMainId(vertexComponent._2.ids)

    // tag1:1,tag2:1,tag3:1
    val tags = vertexComponent._2.tags
      .map(item => item._1 + ":" + item._2)
      .mkString(",")

    Tags(mainId, tags)
  }

  def reduceVertex(curr: VertxComponent, mid: VertxComponent) = {
    val id = curr.id
    val ids = curr.ids
    val temp = curr.tags.map {
      case (key, value) => if (mid.tags.contains(key)) {
        (key, value + mid.tags(key))
      } else {
        (key, value)
      }
    }
    val tags = mid.tags ++ tags

    VertxComponent(id, ids, tags, curr.component)
  }

  def createTags(row: Row): IdsWithTags = {
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

    val ids = getIdMaps(row)
    val mainId = getMainId(ids)

    IdsWithTags(mainId, ids, tags.toMap)
  }

  def getIdMaps(row: Row): Map[String, String] = {
    val keyList = List("imei", "imeimd5", "imeisha1", "mac", "macmd5", "macsha1", "openudid",
      "openudidmd5", "openudidsha1", "idfa", "idfamd5", "idfasha1")

    keyList.map(key => (key, row.getAs[String](key)))
      .filter(item => StringUtils.isNoneBlank(item._2))
      .toMap
  }

  def getMainId(ids: Map[String, String]) = {

    val keyList = List("imei", "imeimd5", "imeisha1", "mac", "macmd5", "macsha1", "openudid",
      "openudidmd5", "openudidsha1", "idfa", "idfamd5", "idfasha1")

    keyList.map(key => ids.get(key))
      .filter(Option => Option.isDefined)
      .map(_.get)
      .head
  }

  def toGeoHash(longitude: Double, latitude: Double) = {
    GeoHash.withCharacterPrecision(latitude, longitude, 8).toBase32
  }

  private val ODS_TABLE = ETLRunner.ODS_TABLE_NAME
  private val AREA_TABLE = BusinessAreaRunner.AREA_TABLE_NAME

}

case class IdsWithTags(mainId: String, ids: Map[String, String], tags: Map[String, Int])

case class Vertx(id: String, ids: Map[String, String], tags: Map[String, Int])

case class Edge(src: String, dst: String)

case class VertxComponent(id: String, ids: Map[String, String], tags: Map[String, Int], component: String)

case class Tags(mainId: String, tags: String)