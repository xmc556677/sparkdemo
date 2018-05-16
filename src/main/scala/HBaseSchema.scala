package cc.xmccc.sparkdemo

import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, HBaseAdmin}
import org.apache.hadoop.hbase.regionserver.KeyPrefixRegionSplitPolicy
import org.apache.hadoop.hbase.util.Bytes
import java.math.BigInteger

object HBaseSchema {

  val conf = HBaseConfiguration.create()
  val conn = ConnectionFactory.createConnection(conf)
  val admin = conn.getAdmin()

  def createNameSpace: Unit = {
    val namespace = NamespaceDescriptor.create("protocol_identify").build()
    admin.createNamespace(namespace)
  }

  def createTestDataTableSchema: Unit = {
    val table = new HTableDescriptor(TableName.valueOf("protocol_identify:pcap_packet_test_data"))
    table.addFamily(new HColumnDescriptor("p"))
    table.setValue(HTableDescriptor.SPLIT_POLICY, classOf[KeyPrefixRegionSplitPolicy].getName)
    table.setValue(KeyPrefixRegionSplitPolicy.PREFIX_LENGTH_KEY, "16")

    admin.createTable(
      table,
      Bytes.toBytes(0.toShort),
      Bytes.toBytes(16.toShort),
      15
    )
  }

  def createTestDataFeatureTableSchema(tablename: String): Unit = {
    val table = new HTableDescriptor(TableName.valueOf(tablename))
    table.addFamily(new HColumnDescriptor("p"))
    table.setValue(HTableDescriptor.SPLIT_POLICY, classOf[KeyPrefixRegionSplitPolicy].getName)
    table.setValue(KeyPrefixRegionSplitPolicy.PREFIX_LENGTH_KEY, "4")

    admin.createTable(
      table,
      getHexSplits(
        Array.fill(4)(1.toByte),
        Array.fill(4)(255.toByte),
        15
      )
    )

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

  def main(args: Array[String]): Unit = {
    val tablename = args(0)
    createTestDataFeatureTableSchema(tablename)
  }
}
