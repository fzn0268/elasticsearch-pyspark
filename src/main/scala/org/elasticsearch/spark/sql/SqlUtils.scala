package org.elasticsearch.spark.sql

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.elasticsearch.hadoop.cfg.ConfigurationOptions
import org.elasticsearch.spark.sparkContextFunctions

import scala.jdk.CollectionConverters.mapAsJavaMapConverter
import scala.language.implicitConversions

object SqlUtils {

  implicit def sparkSessionFunctions(spark: SparkSession) = new SparkSessionFunctions(spark)

  class SparkSessionFunctions(spark: SparkSession) {
    def esDF2(cfg: scala.collection.Map[String, String], schema: StructType): DataFrame = {
      val rdd = spark.sparkContext.esJsonRDD(cfg + (ConfigurationOptions.ES_OUTPUT_JSON -> "true"))
      spark.createDataFrame(rdd.map(r => Row(r._2)), StructType(Seq(StructField("json", StringType))))
        .select(from_json(col("json"), schema).as("row"))
        .select(col("row.*"))
    }

    def esDF2(cfg: scala.collection.Map[String, String]): DataFrame = {
      val schema = MappingUtils.getSchemaOfIndex(spark, cfg.asJava)
      esDF2(cfg, schema)
    }
  }
}
