package org.elasticsearch.spark.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructField, StructType}
import org.elasticsearch.spark.cfg.SparkSettingsManager

import scala.jdk.CollectionConverters.seqAsJavaListConverter

object MappingUtils {
  def getSchemaOfIndex(spark: SparkSession, cfg: java.util.Map[String, String]): StructType = {
    val esConf = new SparkSettingsManager().load(spark.sparkContext.getConf).copy()
    esConf.merge(cfg)
    SchemaUtils.discoverMapping(esConf).struct
  }

  def getSchemaStringOfIndex(spark: SparkSession, cfg: java.util.Map[String, String]): String = getSchemaOfIndex(spark, cfg).toDDL
  def getSchemaFieldListOfIndex(spark: SparkSession, cfg: java.util.Map[String, String]): java.util.List[StructField] = getSchemaOfIndex(spark, cfg).asJava
}
