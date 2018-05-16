package cc.xmccc.sparkdemo

import it.nerdammer.spark.hbase._
import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.api.python.Converter
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.hbase.util.Bytes
import org.json4s._
import org.json4s.jackson.JsonMethods._

import com.fasterxml.jackson.databind.ObjectMapper
import org.msgpack.core.MessagePack
import org.msgpack.jackson.dataformat.MessagePackFactory

import scala.collection.JavaConverters._

case class Slot(cf: String, cl: String, v: String)
case class PyPut(row: String, vpair: List[Slot])

class StringToImmutableBytesWritableConverter extends Converter[Any, ImmutableBytesWritable] {
  override def convert(o: Any): ImmutableBytesWritable  = {
    val obj = o.asInstanceOf[String]
    new ImmutableBytesWritable(obj.getBytes())
  }
}

class ImmutableBytesWritableToByteArrayConverter extends Converter[Any, Array[Byte]] {
  override def convert(obj: Any): Array[Byte] = {
    val row: ImmutableBytesWritable = obj.asInstanceOf[ImmutableBytesWritable]
    row.get()
  }
}

class HbaseResultToMsgPackConverter extends Converter[Any, Array[Byte]] {
  override def convert(obj: Any): Array[Byte] = {
    val result = obj.asInstanceOf[Result]

    val packer = MessagePack.newDefaultBufferPacker()
    val objmapper = new ObjectMapper(new MessagePackFactory())

    packer.packMapHeader(1)
    packer.packBinaryHeader(result.getRow.length)
    packer.writePayload(result.getRow)

    packer.packArrayHeader(result.listCells.asScala.length)
    result.listCells.asScala.foreach { cell =>
      val cf = CellUtil.cloneFamily(cell)
      val cq = CellUtil.cloneQualifier(cell)
      val v  = CellUtil.cloneValue(cell)
      packer.packMapHeader(3)

      packer.packString("cf")
      packer.packBinaryHeader(cf.length)
      packer.writePayload(cf)

      packer.packString("cq")
      packer.packBinaryHeader(cq.length)
      packer.writePayload(cq)

      packer.packString("v")
      packer.packBinaryHeader(v.length)
      packer.writePayload(v)
    }

    packer.toByteArray
  }
}

class HBaseResultsToBytesConverter extends Converter[Any, Array[Byte]] {
  override def convert(obj: Any): Array[Byte] = {
    val result = obj.asInstanceOf[Result]

    val vpairs: List[Byte] = result.listCells().asScala.map{
      cell =>
        CellUtil.cloneFamily(cell).toList ++
        CellUtil.cloneQualifier(cell).toList ++
        CellUtil.cloneValue(cell).toList
    }.toList.flatten

    vpairs.toArray
  }
}

class StringJsonToPutConverter extends Converter[Any, Put] {
  override def convert(o: Any): Put = {
    val json = o.asInstanceOf[String]
    implicit val formats = DefaultFormats
    val obj = parse(json).extractOrElse(PyPut("",Slot("", "", "") :: Nil))

    val p = new Put(Bytes toBytes obj.row)
    obj.vpair.foreach{case Slot(cf, cl ,v) =>
      p.addImmutable(
        Bytes toBytes cf,
        Bytes toBytes cl,
        Bytes toBytes v
      )
    }
    p
  }
}

object SparkHbase {
  val ZOOKEEPER_QUORUM = "hmaster.hbase,hslave04.hbase,hslave08.hbase,hslave12.hbase,hslave15.hbase"

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("SparkHbase")
      .getOrCreate()

    val rdd = sparkSession.sparkContext
      .hbaseTable[(String, String)]("data_test")
      .select("raw")
      .inColumnFamily("packet")

    val rawPacket = rdd.take(50).map{
      case (row, data) =>
        Bytes toBytes data
    }

    while(true) {
      Thread.sleep(2)
    }

    sparkSession.close()
  }
}
