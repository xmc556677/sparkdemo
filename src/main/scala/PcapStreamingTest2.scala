package cc.xmccc.sparkdemo

import cc.xmccc.sparkdemo.PacketUtils.saltTulpe5TimeslotRowkeyStrategy
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming._
import org.pcap4j.packet.EthernetPacket

import scala.util.Try

object PcapStreamingTest2 {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("PcapStreamingTest")
      .getOrCreate()

    val ssc = new StreamingContext(sparkSession.sparkContext, Seconds(1))
    ssc.remember(Minutes(5))
    ssc.checkpoint("hdfs://hmaster.hbase:9000/checkpoint")

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "192.168.0.145:9092",
      "key.deserializer" -> classOf[ByteArrayDeserializer],
      "value.deserializer" -> classOf[ByteArrayDeserializer],
      "group.id" -> "test",
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
      record => {
        val raw_pkt = record.value
        val bucket = Bytes.toLong(record.key) / 1000 / 1000 / 60 toInt
        val eth_maybe_pkt = Try{EthernetPacket.newPacket(raw_pkt, 0, raw_pkt.length)}.toOption

        (bucket, 1)
      }
    }
    val mapingFunc = (bucket: Int, proto: Option[Int], state: State[Long]) => {
      if (state.isTimingOut()) println("timeout")
      (proto, state.getOption()) match {
        case (Some(p), None) => {
          state.update(0)
          None
        }
        case (Some(p), Some(oldv)) => {
          state.update(p +  oldv)
          None
        }
        case (None, _) => {
          Some(state.get())
        }
      }
    }

    val func = StateSpec.function(mapingFunc).timeout(Seconds(2))

    hbase_kv_stream.mapWithState(func).foreachRDD{
      rdd =>
        rdd.collect.foreach{
          case Some(v) => println(v)
          case None =>
        }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
