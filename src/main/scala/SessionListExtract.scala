package cc.xmccc.sparkdemo

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object SessionListExtract {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("SessionListExtract")
      .getOrCreate()

    val dense = args(0).toInt

    val conf = HBaseConfiguration.create()
    conf.set(TableInputFormat.INPUT_TABLE, "protocol_identify:pcap_packet_test_data2")
    conf.set(TableInputFormat.SCAN_COLUMNS, "p")

    sparkSession.sparkContext.getConf
      .registerKryoClasses(Array(classOf[Result]))

    val rdd = sparkSession.sparkContext.newAPIHadoopRDD(conf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]
    )

    val srdd = rdd.map{
      case(r, _) =>
        val row = r.get()
        val salt = row.slice(0, 16)
        val datasetid = row.slice(16, 18)
        val timestamp = row.slice(18, 22)
        val tuple5 = row.slice(26, 39)
        val ts_sec = Bytes.toInt(timestamp.reverse)
        val prefix = ts_sec / dense
        val prefix_bytes = Bytes.toBytes(prefix).reverse

        val bucket = prefix % 100
        val bucket_bytes = Bytes.toBytes(bucket.toShort).reverse

        val rowkey = bucket_bytes ++  prefix_bytes ++ tuple5

        rowkey
    }

    val hbaserdd: RDD[(ImmutableBytesWritable, Put)] = srdd.keyBy(x => Bytes.toString(x)).groupByKey().map{
      case (ks, keys) =>
        val keys_arr = keys.toArray
        val k = keys_arr(0)
        val c = keys_arr.length
        val put = new Put(k)
        val cf = Bytes.toBytes("i")
        put.addImmutable(cf, Bytes toBytes "c", Bytes.toBytes(c))

        (new ImmutableBytesWritable(k), put)
    }

    val nconf = HBaseConfiguration.create()
    val job = Job.getInstance(nconf)
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
    val jobconf = job.getConfiguration
    jobconf.set(TableOutputFormat.OUTPUT_TABLE, "protocol_identify:pcap_packet_test_data2_session")

    hbaserdd.saveAsNewAPIHadoopDataset(jobconf)

    println(rdd.count())

    sparkSession.close()
  }
}
