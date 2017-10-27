package spark.examples.hbase 

import org.apache.spark._
import org.apache.spark.rdd.NewHadoopRDD
import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor}
import org.apache.hadoop.hbase.{CellUtil,TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD

import java.io.File
import java.io.FileInputStream
import org.apache.hadoop.fs.FileUtil
import org.apache.hadoop.conf.Configuration
import java.io.StringWriter

object HBaseTest {
  def main(args: Array[String]) {

    val sparkconf = new SparkConf().setAppName("Simple HBase load")
    val sc =new SparkContext(sparkconf)

    val conf = HBaseConfiguration.create()

    // Other options for configuring scan behavior are available. More information available at 
    // http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/mapreduce/TableInputFormat.html
    conf.set(TableInputFormat.INPUT_TABLE, "bridge:repo") 
    val osw = new StringWriter()
    Configuration.dumpConfiguration(conf,osw)
    println("Start of conf dump")
    //println(osw.toString)
    println("End of print dump")

    // Initialize hBase table if necessary
    val admin = new HBaseAdmin(conf)
    if(!admin.isTableAvailable("bridge:repo")) {
      val tableDesc = new HTableDescriptor("bridge:repo")
      admin.createTable(tableDesc)
    }

    val hRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat], 
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    // rdd returns tuple of ImmutableBytesWritable, Result
    // for ease of viewing data, convert to new RDD with String, String i.e. rowKey(in String), value(in String)
    // we are stripping out the timestamp value from the rowkey as well
    val hBaseRDD = hRDD.map(tuple => tuple._2).map(result => (Bytes.toString(result.getRow()).split(" ")(0), Bytes.toString(result.value)))
   
    // lets output one 
    hBaseRDD.take(1).foreach( x => println("Sample data from newApiHadoopRDD: " + x._1 + " XXX " + x._2))

    
    // function to retrieve rows from hbase table
    def getRec (htable: Table, rowKey : String ) :  (String, String) = {
      var rkey = new Get(Bytes.toBytes(rowKey))
      var rval = htable.get(rkey)
      (rowKey, Bytes.toString(rval.value))
    }

    val readRDD = hBaseRDD.mapPartitions(iter => {
      var conf = HBaseConfiguration.create()
      var inconf = new File("/etc/hbase/conf/hdfs-site.xml")
      var inHbase = new FileInputStream(inconf)
      conf.addResource(inHbase, "hbase-site.xml")
      conf.set(TableInputFormat.INPUT_TABLE, "bridge:repo")
      var connection = ConnectionFactory.createConnection(conf)
      var htable = connection.getTable(TableName.valueOf( Bytes.toBytes("bridge:repo") ) )
      println("Connected!")
      iter.map{ case(x,y) => getRec(htable,x) }
      }
    )
    println("Count" + readRDD.count())
    readRDD.take(1).foreach(x => println("Sample data from gets: " + x._1 + " YYY " + x._2))
  }
}
