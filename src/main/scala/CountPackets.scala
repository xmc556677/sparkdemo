package cc.xmccc.sparkdemo

import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration}
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.pcap4j.packet.{EthernetPacket, IpV4Packet, TcpPacket, UdpPacket}

import scala.collection.JavaConverters._
import scala.util.Try

object CountPackets {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("CountPackets")
      .getOrCreate()

    val logger = LogManager.getRootLogger

    val conf = HBaseConfiguration.create()
    conf.set(TableInputFormat.INPUT_TABLE, "protocol_identify:pcap_packet_test_data")
    conf.set(TableInputFormat.SCAN_COLUMNS, "p")
    sparkSession.sparkContext.getConf
      .registerKryoClasses(Array(classOf[Result]))

    val hBaseRdd = sparkSession.sparkContext.newAPIHadoopRDD(conf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]
    )

    val rawbytesRdd = hBaseRdd map {
      case (_, item) =>
        val row = item.getRow
        val rawpacket = item.getColumnLatestCell(Bytes.toBytes("p"), Bytes.toBytes("r"))
        (row, CellUtil.cloneValue(rawpacket))
    }

/*    val tcpnum =
      rawbytesRdd filter  {
        case (row, rawp) =>
          val ethp = Try{EthernetPacket.newPacket(rawp, 0, rawp.length)}.toOption
          val tcph = for {
            eth <- ethp
            tcp = eth.get(classOf[TcpPacket])
            tcph <- Try{tcp.getHeader}.toOption
          } yield tcph

          tcph match {
            case Some(p) => true
            case None => false
          }
      } count*/

    val tcpnum =
      rawbytesRdd filter  {
        case (row, rawp) =>
          val ethp = Try{EthernetPacket.newPacket(rawp, 0, rawp.length)}.toOption
          ethp match {
            case Some(p) => p.contains(classOf[TcpPacket])
            case None => false
          }
      } count

    val tcpandipv4num =
      rawbytesRdd filter  {
        case (row, rawp) =>
          val ethp = Try{EthernetPacket.newPacket(rawp, 0, rawp.length)}.toOption
          ethp match {
            case Some(p) => p.contains(classOf[IpV4Packet]) && p.contains(classOf[TcpPacket])
            case None => false
          }
      } count

    val udpnum =
      rawbytesRdd filter  {
        case (row, rawp) =>
          val ethp = Try{EthernetPacket.newPacket(rawp, 0, rawp.length)}.toOption
          val udph = for {
            eth <- ethp
            udp = eth.get(classOf[UdpPacket])
            udph <- Try{udp.getHeader}.toOption
          } yield udph

          udph match {
            case Some(p) => true
            case None => false
          }
      } count

    val udpandipv4num =
      rawbytesRdd filter  {
        case (row, rawp) =>
          val ethp = Try{EthernetPacket.newPacket(rawp, 0, rawp.length)}.toOption
          ethp match {
            case Some(p) => p.contains(classOf[IpV4Packet]) && p.contains(classOf[UdpPacket])
            case None => false
          }
      } count

    println("TCP packet number: " + tcpnum)
    println("TCP And IPv4 packet number: " + tcpandipv4num)
    println("UDP packet number: " + udpnum)
    println("UDP And IPv4 packet number: " + udpandipv4num)
  }
}
