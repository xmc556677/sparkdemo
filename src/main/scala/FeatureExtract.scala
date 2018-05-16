package cc.xmccc.sparkdemo

import org.apache.hadoop.hbase.{Cell, CellUtil, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.log4j.LogManager
import org.apache.spark.rdd.PairRDDFunctions
import org.pcap4j.packet.{EthernetPacket, IpV4Packet, TcpPacket, UdpPacket}

import scala.collection.JavaConverters._
import scala.util.Try

object FeatureExtract {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("FeatureExtract")
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

    val etherRdd = hBaseRdd map {
      case (_, item) =>
        val row = item.getRow
        val rawpacket = item.getColumnLatestCell(Bytes.toBytes("p"), Bytes.toBytes("r"))
        val rawp = CellUtil.cloneValue(rawpacket)
        (row, Try{
          EthernetPacket.newPacket(rawp, 0, rawp.length)
        }.toOption)
    } filter {
      case (_, opacket) =>
        opacket match {
          case None => false
          case _ => true
        }
    }

    val ipv4 = etherRdd map {
      case (row, opacket) =>
        for {
          ep <- opacket
          ipv4 = ep.get(classOf[IpV4Packet])
          ipv4h <- Try{ipv4.getHeader}.toOption
        } yield {
          val p = new Put(row)
          val cf = Bytes toBytes "31"
          p.addImmutable(cf, Bytes toBytes "s", ipv4h.getSrcAddr.getAddress)
          p.addImmutable(cf, Bytes toBytes "d", ipv4h.getDstAddr.getAddress)
          p.addImmutable(cf, Bytes toBytes 'v', Array(ipv4h.getVersion.value))
          p.addImmutable(cf, Bytes toBytes "p", Array(ipv4h.getProtocol.value))
          p.addImmutable(cf, Bytes toBytes "0", Array(ipv4h.getIhl))
          p.addImmutable(cf, Bytes toBytes "1", Array(ipv4h.getTos.value))
          p.addImmutable(cf, Bytes toBytes "2", Bytes toBytes ipv4h.getIdentification)
          p.addImmutable(cf, Bytes toBytes "3", Bytes toBytes ipv4h.getTotalLength)
          p.addImmutable(cf, Bytes toBytes "4", Bytes toBytes ipv4h.getDontFragmentFlag)
          p.addImmutable(cf, Bytes toBytes "5", Bytes toBytes ipv4h.getMoreFragmentFlag)
          p.addImmutable(cf, Bytes toBytes "6", Array(ipv4h.getTtl))
          p.addImmutable(cf, Bytes toBytes "7",
            ipv4h.getOptions.asScala.map(_.getRawData).flatten.toArray)

          (new ImmutableBytesWritable(row),
            p)
        }
    } filter {
      _ match {
        case None => false
        case _ => true
      }
    } map {
      _.get
    }

    val tcp = etherRdd map {
      case (row, opacket) =>
        for {
          ep <- opacket
          tcp = ep.get(classOf[TcpPacket])
          tcph <- Try{tcp.getHeader}.toOption
        } yield {
          val p = new Put(row)
          val cf = Bytes toBytes "41"
          p.addImmutable(cf, Bytes toBytes "s", Bytes toBytes tcph.getSrcPort.value)
          p.addImmutable(cf, Bytes toBytes "d", Bytes toBytes tcph.getDstPort.value)
          p.addImmutable(cf, Bytes toBytes "r", Array(tcph.getReserved))
          p.addImmutable(cf, Bytes toBytes "o",
            tcph.getOptions.asScala.map(_.getRawData).flatten.toArray)
          p.addImmutable(cf, Bytes toBytes "f", tcph.getRawData.slice(13, 14))

          (new ImmutableBytesWritable(row),
            p)
        }
    } filter {
      _ match {
        case None => false
        case _ => true
      }
    } map {
      _.get
    }

    val udp = etherRdd map {
      case (row, opacket) =>
        for {
          ep <- opacket
          udp = ep.get(classOf[UdpPacket])
          udph <- Try{udp.getHeader}.toOption
        } yield {
          val p = new Put(row)
          val cf = Bytes toBytes "42"
          p.addImmutable(cf, Bytes toBytes "s", Bytes toBytes udph.getSrcPort.value)
          p.addImmutable(cf, Bytes toBytes "d", Bytes toBytes udph.getDstPort.value)
          p.addImmutable(cf, Bytes toBytes "l", Bytes toBytes udph.getLength)

          (new ImmutableBytesWritable(row),
            p)
        }
    } filter {
      _ match {
        case None => false
        case _ => true
      }
    } map {
      _.get
    }

    val job = Job.getInstance(conf)
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
    val jobconf = job.getConfiguration
    jobconf.set(TableOutputFormat.OUTPUT_TABLE, "protocol_identify:pcap_packet_test_data_feature")

    new PairRDDFunctions(ipv4).saveAsNewAPIHadoopDataset(jobconf)
    logger.info("IPv4 Extracted")
    new PairRDDFunctions(tcp).saveAsNewAPIHadoopDataset(jobconf)
    logger.info("TCP Extracted")
    new PairRDDFunctions(udp).saveAsNewAPIHadoopDataset(jobconf)
    logger.info("UDP Extracted")

    sparkSession.close()
  }
}
