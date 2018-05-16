package cc.xmccc.sparkdemo

import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.{PairRDDFunctions, RDD}
import org.apache.spark.sql.SparkSession
import java.io._

import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.mapred.JobConf

import scala.collection.JavaConverters._

object ExtractTuple5 {

  val ZOOKEEPER_QUORUM = "hmaster.hbase,hslave04.hbase,hslave08.hbase,hslave12.hbase,hslave15.hbase"

  val sparkSession = SparkSession.builder()
    .appName("ExtractTuple5")
    .getOrCreate()

  def getRddFromHbaseTable(table: String): RDD[(String, Array[Byte])] = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", ZOOKEEPER_QUORUM)
    conf.set(TableInputFormat.INPUT_TABLE, table)
    val hBaseRdd = sparkSession.sparkContext.newAPIHadoopRDD(
      conf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]
    )

    hBaseRdd.map{
      case (row, item) =>
        val r: String = Bytes toString row.get()
        val cell = item
          .getColumnCells(Bytes.toBytes("packet"), Bytes.toBytes("raw"))
          .asScala
          .head
        val cv: Array[Byte]= CellUtil.cloneValue(cell)
        (r, cv)
    }
  }

  def saveRddToHbaseTable(table: String, rdd: RDD[(String, (Int, Short, Int, Short, Byte), Array[Byte])] ): Unit = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", ZOOKEEPER_QUORUM)
    conf.set(TableOutputFormat.OUTPUT_TABLE, table)

    val jobConf = new JobConf(conf, this.getClass)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, table)
    new PairRDDFunctions(rdd.map{
      case (row, tuple5, raw) =>
        val p = new Put(Bytes toBytes row)
        val tuple5raw = new ByteArrayOutputStream()
        val oos = new ObjectOutputStream(tuple5raw)
        oos.writeObject(tuple5)
        p.addImmutable(
          Bytes toBytes "packet",
          Bytes toBytes "tuple5",
          tuple5raw.toByteArray()
        )
        p.addImmutable(
          Bytes toBytes "packet",
          Bytes toBytes "raw",
          raw
        )
        new Tuple2[ImmutableBytesWritable, Put](
          new ImmutableBytesWritable(row.getBytes()),
          p
        )
    }).saveAsHadoopDataset(jobConf)

  }

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().getOrCreate()
    val packetRdd: RDD[(String, Array[Byte])] = getRddFromHbaseTable("data_test")
    val tuple5andraw: RDD[(String, (Int, Short, Int, Short, Byte), Array[Byte])] =
      packetRdd.map{
        case (row, raw) =>
          val saddr = Bytes toInt  raw.slice(0x1A, 0x1E)
          val daddr = Bytes toInt raw.slice(0x1E, 0x22)
          val sport = Bytes toShort raw.slice(0x22, 0x24)
          val dport = Bytes toShort raw.slice(0x24, 0x26)
          val Array(proto) = raw.slice(0x17, 0x18)
          (row, (saddr, sport, daddr, dport, proto), raw)
      }
    saveRddToHbaseTable("test:test", tuple5andraw)
    tuple5andraw.take(10).foreach{
      case (row, (saddr, sport, daddr, dport, proto), raw) =>
        println(row)
        Bytes toBytes saddr map print
        println()
        Bytes toBytes daddr map print
        println()
        println(sport)
        println(dport)
        println(proto)
    }
  }
}