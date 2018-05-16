package cc.xmccc.sparkdemo

import java.security.MessageDigest

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.SparkSession
import org.pcap4j.core.{PcapHandle, Pcaps}
import org.pcap4j.packet._

import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.reflect.io.File
import scala.util.{Failure, Success, Try}


object PutPcapToHbase {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("PutPcapToHbase")
      .getOrCreate()

    val filepath = args(0)
    val maxbufsize = args(1).toInt
    val sessiondense = args(2).toInt
    val datasetid = args(3).toShort

    val pcap = Pcaps.openOffline(filepath)
    println("Starting ")
    val filesize = File(args(0)).length

/*    val pcapfile = Source.fromFile(filepath)
    val globalheader = pcapfile.take(24).map(_.toByte).toStream
    val magic_number = globalheader.slice(0,4)
    val version_major = globalheader.slice(4,6)
    val version_minor = globalheader.slice(6, 8)
    val thiszone = globalheader.slice(8, 12)
    val sigfigs = globalheader.slice(12, 16)
    val snaplen = globalheader.slice(16, 20)
    val network = globalheader.slice(20, 24)
    pcapfile.close()*/

    val conf = HBaseConfiguration.create()
    val job = Job.getInstance(conf)
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
    val jobconf = job.getConfiguration
    jobconf.set(TableOutputFormat.OUTPUT_TABLE, "protocol_identify:pcap_packet_test_data2")

    val it = new MaxLengthPacketIterator(pcap, maxbufsize)
    for( packetinfos <- it) {
      println("declare boradcast")
      println("length of packetinfo: " + packetinfos.length)
      val broad = sparkSession.sparkContext.broadcast(packetinfos)
      val rdd = sparkSession.sparkContext.parallelize(0 to broad.value.length-1, 32)
      val packetrdd = rdd.map(i => broad.value(i))
      val hbaserdd = packetrdd.map {
        case(ts_sec, ts_usec, orig_len, rp) => {
          val ts_sec_bytes = Bytes.toBytes(ts_sec)
          val ts_usec_bytes = Bytes.toBytes(ts_usec)
          val orig_len_bytes = Bytes.toBytes(orig_len)
          val rawpacket = rp
          val pkt = EthernetPacket.newPacket(rp, 0, rp.length)

          //extract ipv4 header
          val ipv4p = pkt.get(classOf[IpV4Packet])
          val (sip, dip, proto): (Array[Byte], Array[Byte], Array[Byte]) = Try{ipv4p.getHeader} match {
            case Success(h) =>
              (h.getSrcAddr.getAddress, h.getDstAddr.getAddress, Array(h.getProtocol.value()))
            case Failure(m) =>
              val byte4z: Array[Byte]= Array(0, 0, 0, 0)
              (byte4z, byte4z, Array(0.toByte))
          }

          val tcpp = pkt.get(classOf[TcpPacket])
          val udpp = pkt.get(classOf[UdpPacket])
          val (sport, dport): (Array[Byte], Array[Byte])= (Try{tcpp.getHeader}, Try{udpp.getHeader}) match {
            case (Failure(_), Failure(_)) =>
              val bytes2z: Array[Byte] = Array(0, 0)
              (bytes2z, bytes2z)
            case (Success(h), _) =>
              (Bytes.toBytes(h.getSrcPort.value).reverse, Bytes.toBytes(h.getDstPort.value))
            case (_, Success(h)) =>
              (Bytes.toBytes(h.getSrcPort.value).reverse, Bytes.toBytes(h.getDstPort.value))
          }

          val tuple5: Array[Byte] = sip ++ dip ++ sport ++ dport ++ proto
          val salt = MessageDigest.getInstance("MD5").digest(tuple5)
          val timestamp = ts_sec_bytes ++ ts_usec_bytes
          val datasetidbytes = Bytes.toBytes(datasetid)
          val hash = Array.fill(4)((scala.util.Random.nextInt(256) - 128).toByte)

          val rowkey = salt ++ datasetidbytes ++ timestamp ++ tuple5 ++ hash
          val put = new Put(rowkey)
          val cf = Bytes.toBytes("p")
          put.addImmutable(cf, Bytes toBytes "r", rawpacket)
          put.addImmutable(cf, Bytes toBytes "l", orig_len_bytes)

          (new ImmutableBytesWritable(rowkey), put)
        }
      }
      hbaserdd.saveAsNewAPIHadoopDataset(jobconf)
      broad.destroy()
    }

  }

  class MaxLengthPacketIterator(val pcapHandle: PcapHandle, val max_size: Int) extends Iterator[Array[(Int, Int, Int, Array[Byte])]] {
    type PacketInfo = (Int, Int, Int, Array[Byte])
    var isend = false

    override def hasNext: Boolean = !isend

    override def next(): Array[(Int, Int, Int, Array[Byte])] = {
      var length = 0
      val arrayBuff = ArrayBuffer.empty[PacketInfo]
      while(length < max_size && (! isend)) {
        Try{pcapHandle.getNextPacketEx} match {
          case Success(p) => {
            length += p.getRawData.length
            arrayBuff.append((
              pcapHandle.getTimestamp.getTime / 1000 toInt,
              pcapHandle.getTimestamp.getNanos / 1000 toInt,
              pcapHandle.getOriginalLength,
              p.getRawData
            ))
          }
          case Failure(p) => {
            isend = true
          }
        }
      }
      arrayBuff.toArray
    }
  }
}
