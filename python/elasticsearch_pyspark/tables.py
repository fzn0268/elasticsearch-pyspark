from pyspark.sql import types, Column, DataFrame, functions, SparkSession
from pyspark.sql.types import StructField, Row
from pyspark.sql.functions import *

from typing import Dict


def get_schema_of_index(
    cfg: Dict[str, str],
    spark: Optional[SparkSession] = None
) -> DataType:
    if not spark:
        spark = SparkSession.getActiveSession()
    j_spark_session = spark._jsparkSession
    jvm = spark._jvm
    jcfg = jvm.java.util.HashMap()
    for k, v in cfg.items():
        jcfg.put(k, v)
    schemaStr = jvm.org.elasticsearch.spark.sql.MappingUtils.getSchemaStringOfIndex(j_spark_session, jcfg)
    return types._parse_datatype_string(schemaStr)


def es_df(
    cfg: Dict[str, str],
    schema: Union[StructType, str] = None,
    spark: Optional[SparkSession] = None
) -> DataFrame:
    if not spark:
        spark = SparkSession.getActiveSession()
    if not schema:
        schema = get_schema_of_index(cfg, spark)
    cfg['es.output.json'] = 'true'
    rdd = spark.sparkContext.newAPIHadoopRDD("org.elasticsearch.hadoop.mr.EsInputFormat",
                                             "org.apache.hadoop.io.NullWritable",
                                             "org.elasticsearch.hadoop.mr.LinkedMapWritable",
                                             conf=cfg)
    return spark.createDataFrame(rdd.map(lambda r: Row(json=r[1])), 'json STRING')\
        .select(from_json(col('json'), schema).alias('row')).select(col('row.*'))