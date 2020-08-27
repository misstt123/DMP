package com.itcast.dmp.utils

import com.typesafe.config.ConfigFactory
import okhttp3.{OkHttpClient, Request}
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

object HttpUtils {
  private val config = ConfigFactory.load("common")
  private val key: String = config.getString("amap.key")
  private val apiUrl = config.getString("amap.apiUrl")
  private val httpCliet = new OkHttpClient()

  def getLocationInfo(longitude: Double, latitude: Double): Option[String] = {

    val url = s"${apiUrl}v3/geocode/regeo?" +
      s"location=$longitude,$latitude&key=${key}"

    val request = new Request.Builder()
      .url(url)
      .get()
      .build()

    try {
      val response = httpCliet.newCall(request).execute()
      if (response.isSuccessful) {
        Some(response.body().string())

      } else None
    } catch {
      case _: Exception => None
    }

  }


  def parseJosn(json: String) = {
    implicit val value = Serialization.formats(NoTypeHints)
    val location: AMapLocation = Serialization.read[AMapLocation](json)
    location
  }
}
// 1. 拷贝要解析的字符串, 去掉无用内容
//    val json =
//      """
//        |{
//        |	"regeocode": {
//        |		"addressComponent": {
//        |			"businessAreas": [{
//        |				"location": "116.470293,39.996171",
//        |				"name": "望京",
//        |				"id": "110105"
//        |			}]
//        |		}
//        |	}
//        |}
//      """.stripMargin
case class AMapLocation(regeocode: Option[RegeoCode])

case class RegeoCode(addressComponent: Option[AddressComponent])

case class AddressComponent(businessAreas: Option[List[BusinessArea]])

case class BusinessArea(name: String)


