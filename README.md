Helper functions for creating DataFrame of ElasticSearch index with complex properties.

The parser in `elasticsearch-spark` package may deserialize out corrupted row data that don't match the schema parsed from index mapping in order to raise exception when materializing DataFrame, however it can be read as RDD of JSON then deserialize with `from_json` function, which is implemented in this library.

Usage:

1. Package this library:

```shell
$ ./sbt/sbt package
```

2. Your code may like this:

```python
from elasticsearch_pyspark.tables import es_df

cfg = {
    'es.nodes': 'x.x.x.x',
    'es.query': '?q=field:value',
    'es.resource': 'myindex'
}

df = es_df(cfg)
df.show(5)
...
```

3. Configure Spark with parameters when submitting:
```shell
$ ES_SPARK_PKG=org.elasticsearch:elasticsearch-spark-30_2.12:x.x.x
$ ES_PYSPARK_JAR=/path/to/elasticsearch-pyspark-30_2.12-1.0-SNAPSHOT.jar
$ spark-submit <your configs> --packages $ES_SPARK_PKG --jars $ES_PYSPARK_JAR --py-files $ES_PYSPARK_JAR <your other params>
```
