package cc.xmccc.sparkdemo

import java.io.OutputStream
import java.math.BigInteger
import java.net.InetAddress
import java.security.MessageDigest

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.util.Bytes
import org.pcap4j.core.{PcapHandle, Pcaps}
import org.pcap4j.packet._

import scala.collection.immutable.Stream.{cons, empty}
import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success, Try}

object Test {
  def main(args: Array[String]): Unit = {
    /*    val raw = "01|00|5e|7f|ff|fa|74|29|af|35|61|93|08|00|45|00|00|9d|30|42|00|00|04|11|5a|67|c0|a8|7b|04|ef|ff|ff|fa|de|df|07|6c|00|89|a1|1a|4d|2d|53|45|41|52|43|48|20|2a|20|48|54|54|50|2f|31|2e|31|0d|0a|48|4f|53|54|3a|20|32|33|39|2e|32|35|35|2e|32|35|35|2e|32|35|30|3a|31|39|30|30|0d|0a|4d|41|4e|3a|20|22|73|73|64|70|3a|64|69|73|63|6f|76|65|72|22|0d|0a|4d|58|3a|20|35|0d|0a|53|54|3a|20|75|72|6e|3a|73|63|68|65|6d|61|73|2d|75|70|6e|70|2d|6f|72|67|3a|64|65|76|69|63|65|3a|4d|65|64|69|61|52|65|6e|64|65|72|65|72|3a|31|0d|0a|0d|0a"
    val framer = new EthernetFramer()
    val rawbytes = raw.split('|').map(x => Integer.parseInt(x, 16).toByte)
    val buff = Buffers.wrap(rawbytes)
    val macp = framer.frame(null, buff)
    println(macp.getPacket(Protocol.TCP).asInstanceOf[TCPPacket].getDestinationPort)*/
    //val ep = EthernetPacket.newPacket(rawbytes, 0, rawbytes.length)
    //println(ep.getPayload.get(classOf[UdpPacket]).getHeader.getDstPort.valueAsInt())
    //println(rawbytes.slice(2,3)(0))

    //val pcap = Pcaps.openOffline(args(0))

    /*    val pcap = Pcap.openStream(args(0))
    val eth = new EthernetFramer()

    pcap.loop(new PacketHandler {
      override def nextPacket(packet: Packet): Boolean = {
        if (packet.hasProtocol(Protocol.TCP)) {
          val tcp = packet.getPacket(Protocol.TCP).asInstanceOf[TCPPacket]
          val ipv4 = packet.getPacket(Protocol.IPv4).asInstanceOf[IPv4Packet]
          println(tcp)
          println(ipv4.getSourceIP + ":" + tcp.getSourcePort)
        }
        true
      }
    })*/
/*    val it = new MaxLengthPacketIterator(pcap, args(1).toInt)

    for((ts, tms, l, p) <- it.next) {
      val tcpp = p.get(classOf[TcpPacket])
      val udpp = p.get(classOf[UdpPacket])
      (Try{tcpp.getHeader}, Try{udpp.getHeader}) match {
        case (Failure(_), Failure(_)) =>
          println("None")
        case (Success(h), _) =>
          val a = Bytes.toBytes(h.getDstPort.value())
          a.foreach(x => print(x.toHexString + " "))
          println()
        case (_, Success(h)) =>
          println(h.getDstPort)
      }
    }
    println(Bytes.toString(Array.fill(16)(0xff.toByte)))*/

/*    val b = getHexSplits(Bytes.toBytes(0.toShort), Bytes.toBytes(100.toShort), 15)
    Bytes.toBytes(100.toShort).foreach(x => print(x + " "))
    println()
    b(0).foreach(x => print(x + " "))*/

/*    var size = 0

    for{
      lp <- travese(pcap)
      p = lp.length
    } size += p

    println(size)*/

    //val b = Array(0xf5, 0xf5).map(_.toByte)
    val b = Bytes toBytes 0xffff
    val b2 = BigInt(0xffff).toByteArray
    val bb = (Bytes toLong Array.fill(8)(0xfe.toByte))
    val bb_b = Bytes toBytes bb
    val bb_big = BigInt(Array.fill(8)(0.toByte)++ Array.fill(8)(0xfe.toByte)) / 100
    val bb_big2 = Bytes.toBytes(bb_big.toLong)

    println(bb_big2.map(x => (x & 0x00ff).toHexString).mkString(" "))
    println(bb_b.map(x => (x & 0x00ff).toHexString).mkString(" "))
    println(Bytes toShort b )
    println(b.map(x => (x & 0x00ff).toHexString).mkString(" "))
    println(b2.map(x => (x & 0x00ff).toHexString).mkString(" "))
    println(BigInt(bb) mod 20)
  }

  def implicit_add(b: Int)(implicit d: Int) = b + d

  def travese(pcapHandle: PcapHandle): Stream[Array[Byte]] = {
    val po = Try{pcapHandle.getNextRawPacketEx}
    po match {
      case Success(p) => cons(p, travese(pcapHandle))
      case Failure(p) => {
        println(p.getMessage)
        empty
      }
    }
  }

  class MaxLengthPacketIterator(val pcapHandle: PcapHandle, val max_size: Int) extends Iterator[List[(Int, Int, Int, Packet)]] {
    type PacketInfo = (Int, Int, Int, Packet)
    var isend = false

    override def hasNext: Boolean = !isend

    override def next(): List[(Int, Int, Int, Packet)] = {
      var length = 0
      val arrayBuff = ArrayBuffer.empty[PacketInfo]
      while(length < max_size && ! isend) {
        Try{pcapHandle.getNextPacketEx} match {
          case Success(p) => {
            length += p.getRawData.length
            arrayBuff.append((
              pcapHandle.getTimestamp.getTime / 1000 toInt,
              pcapHandle.getTimestamp.getNanos / 1000 toInt,
              pcapHandle.getOriginalLength,
              p
            ))
          }
          case Failure(p) => {
            println("Endor")
            isend = true
          }
        }
      }
      arrayBuff.toList
    }
  }

  def getHexSplits(startKey: Array[Byte], endKey: Array[Byte], numRegions: Int): Array[Array[Byte]] = {
    val splits = new Array[Array[Byte]](numRegions - 1)
    var lowestKey = new BigInteger(startKey)
    val highestKey = new BigInteger(endKey)
    val range = highestKey.subtract(lowestKey)
    val regionIncrement = range.divide(BigInteger.valueOf(numRegions))
    lowestKey = lowestKey.add(regionIncrement)
    var i = 0
    while ( {
      i < numRegions - 1
    }) {
      val key = lowestKey.add(regionIncrement.multiply(BigInteger.valueOf(i)))
      val keyByte = key.toByteArray
      splits(i) = Array.fill(endKey.length - keyByte.length)(0.toByte) ++ keyByte

      {
        i += 1; i - 1
      }
    }
    splits
  }
}

