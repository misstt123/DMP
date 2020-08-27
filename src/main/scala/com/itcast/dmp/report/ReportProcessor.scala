package com.itcast.dmp.report

import org.apache.spark.sql.DataFrame
import org.apache.kudu.Schema
trait ReportProcessor {
  def process(dataFrame: DataFrame): DataFrame

  def sourceTableName: String

  def targetTableName: String

  def targetTableSchema: Schema

  def targetTableKeys: List[String]
}
