# Collection of Spark Examples

To compile:

```mvn clean package```

This will build a fat jar with a required dependencies.

To run:

```spark2-submit --deploy-mode client  --conf spark.executor.extraClassPath=/etc/hbase/conf --conf spark.driver.extraClassPath=/etc/hbase/conf --class org.apache.spark.examples.HBaseTest SparkStreamingScenarios.jar logs logs ```
