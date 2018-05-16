package cc.xmccc.sparkdemo

import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.pcap4j.packet.{EthernetPacket, IpV4Packet}
import cc.xmccc.sparkdemo.PacketUtils.saltTulpe5TimeslotRowkeyStrategy
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job

import scala.collection.JavaConverters._
import scala.util.Try

object PcapStreamingTest { def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("PcapStreamingTest")
      .getOrCreate()

    val args = sparkSession.sparkContext.getConf.get("spark.test")
    println(args)

    sparkSession.close()

    val ssc = new StreamingContext(sparkSession.sparkContext, Seconds(1))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "192.168.0.145:9092",
      "key.deserializer" -> classOf[ByteArrayDeserializer], "value.deserializer" -> classOf[ByteArrayDeserializer],
      "group.id" -> "test11",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("test")

    val stream = KafkaUtils.createDirectStream[Array[Byte], Array[Byte]](
      ssc,
      PreferConsistent,
      Subscribe[Array[Byte], Array[Byte]](topics, kafkaParams)
    )

    val hbase_kv_stream = stream.map{
      record =>
        val raw_pkt = record.value
        val eth_maybe_pkt = Try{EthernetPacket.newPacket(raw_pkt, 0, raw_pkt.length)}.toOption
        val rowkey = saltTulpe5TimeslotRowkeyStrategy(0, eth_maybe_pkt, record.timestamp, 60)

        val put = new Put(rowkey)
        val cf = Bytes.toBytes("p")
        put.addImmutable(cf, Bytes toBytes "r", raw_pkt)

        (new ImmutableBytesWritable(rowkey), put)
    }

/*    val job = Job.getInstance(conf)
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
    val jobconf = job.getConfiguration
    jobconf.set(TableOutputFormat.OUTPUT_TABLE, "test001")*/

    hbase_kv_stream.foreachRDD{
      rdd =>
        rdd.foreachPartition{
          (it) => {
            val conf = HBaseConfiguration.create()
            val puts = it.map(_._2).toList.asJava
            val conn = ConnectionFactory.createConnection(conf)
            val table = conn.getTable(TableName.valueOf("test001"))
            table.put(puts)
            table.close()
            conn.close()
          }
        }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
