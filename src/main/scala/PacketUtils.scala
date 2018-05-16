package cc.xmccc.sparkdemo

import java.security.MessageDigest

import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.pcap4j.packet.{IpV4Packet, Packet, TcpPacket, UdpPacket}

import scala.util.{Failure, Success, Try}

case class PacketTuple5(val dip: Array[Byte],
                        val sip: Array[Byte],
                        val dport: Array[Byte],
                        val sport: Array[Byte],
                        val proto: Array[Byte])

object PacketUtils {
  type bytearray = Array[Byte]
  def extractTuple5fromPacket(maybe_pkt: Option[Packet]): PacketTuple5 = {
    val ipv4_dst_src_proto = for {
      pkt <- maybe_pkt
      ipv4_pkt <- Try{pkt.get(classOf[IpV4Packet])}.toOption
      ipv4_hdr <- Try{ipv4_pkt.getHeader}.toOption
    } yield {
      (ipv4_hdr.getDstAddr, ipv4_hdr.getSrcAddr, ipv4_hdr.getProtocol)
    }

    val (dip: bytearray, sip: bytearray, proto: bytearray) = ipv4_dst_src_proto match {
      case Some((dst_obj, src_obj, proto_obj)) => {
        val dip_bytes = dst_obj.getAddress
        val sip_bytes = src_obj.getAddress
        val proto_bytes = Array(proto_obj.value.toByte)
        //require(dip_bytes.length == 4 && sip_bytes.length == 4 && proto_bytes.length == 1)
        (dip_bytes, sip_bytes, proto_bytes)
      }
      case None => {
        val bytes4 = Array.fill(4)(0.toByte)
        val bytes1 = Array.fill(1)(0.toByte)
        (bytes4, bytes4, bytes1)
      }
    }

    val dport_sport: Option[(Short, Short)] =
      (for {
        pkt <- maybe_pkt
        tcp_pkt <- Try{pkt.get(classOf[TcpPacket])}.toOption
      } yield tcp_pkt) match {
      case Some(tcp_pkt) => for {
        tcp_hdr <- Try{tcp_pkt.getHeader}.toOption
      } yield (tcp_hdr.getDstPort.value, tcp_hdr.getSrcPort.value)

      case None => for {
        pkt <- maybe_pkt
        udp_pkt <- Try{pkt.get(classOf[UdpPacket])}.toOption
        udp_hdr <- Try{udp_pkt.getHeader}.toOption
      } yield (udp_hdr.getDstPort.value, udp_hdr.getSrcPort.value)
    }

    val (dport: bytearray, sport: bytearray) = dport_sport match {
      case Some((dp: Short, sp: Short)) => {
        val dport_bytes = Bytes.toBytes(dp)
        val sport_bytes = Bytes.toBytes(sp)

        (dport_bytes, sport_bytes)
      }

      case None =>
        val bytes2 = Array.fill(2)(0.toByte)
        (bytes2, bytes2)
    }

    PacketTuple5(dip, sip, dport, sport, proto)
  }

  def saltTulpe5TimeslotRowkeyStrategy(datasource_id: Short, pkt: Option[Packet], time: Long, timedense: Int): Array[Byte] = {
    val PacketTuple5(dip, sip, dport, sport, proto) = extractTuple5fromPacket(pkt)
    val ts_secs = (time / 1000).toInt

    val tuple5 = dip ++ sip ++ dport ++ sport ++ proto
    val timeslot = ts_secs / timedense
    val hash = Array.fill(4)((scala.util.Random.nextInt(256) - 128).toByte)
    val t_secs_bytes = Bytes.toBytes(time)
    val timeslot_bytes = Bytes.toBytes(timeslot)
    val salt = MessageDigest.getInstance("MD5").digest(tuple5 ++ timeslot_bytes)
    val datasource_id_bytes = Bytes.toBytes(datasource_id)
    val rowkey = salt ++ t_secs_bytes ++ datasource_id_bytes ++ tuple5 ++ hash

    rowkey
  }

}
