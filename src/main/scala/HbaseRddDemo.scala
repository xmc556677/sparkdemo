package cc.xmccc.sparkdemo

import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
//import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
//import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.{PairRDDFunctions, RDD}
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._

object HbaseRddDemo {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession
      .builder()
      .appName("HbaseRddDemo")
      .getOrCreate()

    val conf = HBaseConfiguration.create()
    val ZOOKEEPER_QUORUM = "hmaster.hbase,hslave04.hbase,hslave08.hbase,hslave12.hbase,hslave15.hbase"
    conf.set("hbase.zookeeper.quorum", ZOOKEEPER_QUORUM)
    conf.set(TableInputFormat.INPUT_TABLE, "test:test")
    conf.set(TableInputFormat.SCAN_COLUMNS, "f1")
    sparkSession.sparkContext.getConf
      .registerKryoClasses(Array(classOf[Result]))

    val hBaseRdd = sparkSession.sparkContext.newAPIHadoopRDD(conf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]
    )

    val srdd = hBaseRdd.map{
      item =>
        val a = item._2
        val b = a.getColumnCells(Bytes.toBytes("f1"), Bytes.toBytes("name")).asScala.head
        (Bytes toString item._1.get(), Bytes toString CellUtil.cloneValue(b))
    }.persist()

    //val jobConf = new JobConf(conf, this.getClass)
    //jobConf.setOutputFormat(classOf[TableOutputFormat])
    //jobConf.set(TableOutputFormat.OUTPUT_TABLE, "test:test")
    val job = Job.getInstance(conf)
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
    val jobconf = job.getConfiguration
    jobconf.set(TableOutputFormat.OUTPUT_TABLE, "test:test")
    TableInputFormat.SCAN_ROW_START
    jobconf.set("mapreduce.output.fileoutputformat.outputdir", "/tmp")

    val orderedRdd = RDD.rddToOrderedRDDFunctions(srdd)

    orderedRdd.sortByKey(true)

    new PairRDDFunctions(srdd.map{ case (row, value) =>
      val p = new Put(Bytes toBytes row)
      p.addImmutable(
        Bytes toBytes "f1",
        Bytes toBytes "name",
        Bytes toBytes s"just not test haha at $row"
      )
      new Tuple2[ImmutableBytesWritable, Put](new ImmutableBytesWritable(row.getBytes()), p)
    }).saveAsNewAPIHadoopDataset(jobconf)

    srdd.foreach(println)
    println(srdd.count())

    while(true) {
      Thread.sleep(2)
    }
    sparkSession.stop()
  }
}
