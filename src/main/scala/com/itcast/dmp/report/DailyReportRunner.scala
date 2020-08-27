package com.itcast.dmp.report

import org.apache.spark.sql.SparkSession

object DailyReportRunner {

  def main(args: Array[String]): Unit = {
    import com.itcast.dmp.utils.SparkConfigHelper._
    val spark = SparkSession.builder()
      .master("local[6]")
      .appName("dailyReport")
      .loadConfig()
      .getOrCreate()

    val processores = List[ReportProcessor](
      NewRegionReportProcessor,
      AdsRegionReportProcessor
    )

    for (processor <- processores) {
      import com.itcast.dmp.utils.KuduHelper._
      val dataset = spark.readKuduTable(processor.sourceTableName)
      if (dataset.isDefined) {

        val df = processor.process(dataset.get)
        df.createKuduTable(processor.targetTableName, processor.targetTableSchema, processor.targetTableKeys)
        df.saveToKudu(processor.targetTableName)

      }


    }

  }
}
