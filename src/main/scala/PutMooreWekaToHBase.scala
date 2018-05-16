package cc.xmccc.sparkdemo

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.SizeEstimator
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.io.Source

case class TcpFlow(sparse: Boolean, values: Array[String], weight: Float)

object PutMooreWekaToHBase {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("PutMooreWekaToHBase")
      .getOrCreate()

    sparkSession.sparkContext.getConf
      .registerKryoClasses(Array(classOf[Array[Array[String]]]))

    val filepath = args(0)
    val initnum = args(1)

    implicit val defaltForamt = DefaultFormats

    val rawfile = Source.fromFile(filepath).getLines().mkString
    val tcpflowvalues: Array[Array[String]] = (parse(rawfile) \ "data").extract[Array[TcpFlow]].map{
      case(TcpFlow(_, values, _)) => values
    }

    val conf = HBaseConfiguration.create()
    val job = Job.getInstance(conf)
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
    val jobconf = job.getConfiguration
    jobconf.set(TableOutputFormat.OUTPUT_TABLE, "protocol_identify:moore_dataset")

    val broaddata = sparkSession.sparkContext.broadcast(tcpflowvalues)

    val rdd = sparkSession.sparkContext.parallelize(0 to tcpflowvalues.length-1, 32)

    val hbaserdd = rdd.map(i => (i, broaddata.value(i))).map {
      case (index, values) => {
        val iv = values zip (0 to values.length - 1)
        val row = (Bytes.toBytes((index / 1200) % 15 toShort)) ++ (Bytes toBytes index)
        val put = new Put(row)
        val cf = Bytes toBytes "i"

        for( (v, i) <- iv) {
          put.addImmutable(cf, Bytes toBytes (i.toShort), Bytes toBytes v)
        }

        (new ImmutableBytesWritable(row), put)
      }
    }

    hbaserdd.saveAsNewAPIHadoopDataset(jobconf)
    broaddata.destroy()

    sparkSession.close()
  }
}
