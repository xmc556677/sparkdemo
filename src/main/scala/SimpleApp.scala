package cc.xmccc.sparkdemo
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Scan}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.SparkSession
import it.nerdammer.spark.hbase._

import scala.collection.JavaConverters._

object SimpleApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()

    val conf = HBaseConfiguration.create()
    val ZOOKEEPER_QUORUM = "hmaster.hbase,hslave04.hbase,hslave08.hbase,hslave12.hbase,hslave15.hbase"
    conf.set("hbase.zookeeper.quorum", ZOOKEEPER_QUORUM)

    val connection = ConnectionFactory.createConnection(conf)
    val table = connection.getTable(TableName.valueOf(Bytes.toBytes("test:test")))

    val rdd = spark.sparkContext.parallelize(1 to 1000).map(i => (i.toString, i+1))
    rdd.toHBaseTable("test:test")
      .toColumns("name")
      .inColumnFamily("f1")
      .save()

    val scan = table.getScanner(new Scan())
    scan.asScala.foreach { result =>
      val cells = result.rawCells
      print(Bytes.toString(result.getRow) + " : ")
      for(cell <- cells) {
        val col_name = Bytes.toString(CellUtil.cloneQualifier(cell))
        val col_value = Bytes.toString(CellUtil.cloneValue(cell))
        print(s"($col_name, $col_value)")
      }
    }

    spark.stop()
  }
}
