# Collection of Spark Examples

To compile:

```mvn clean package```

This will build a fat jar with a required dependencies.

To run:

```spark2-submit --deploy-mode client  --conf spark.executor.extraClassPath=/etc/hbase/conf --conf spark.driver.extraClassPath=/etc/hbase/conf --class spark.examples.hbase.HBaseTest spark-examples.jar logs```
