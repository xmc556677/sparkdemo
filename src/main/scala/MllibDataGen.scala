package cc.xmccc.sparkdemo

import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.random.{PoissonGenerator, RandomRDDs}
import org.apache.spark.mllib.random.RandomDataGenerator
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.util.KMeansDataGenerator
import it.nerdammer.spark.hbase._

object MllibDataGen {

  class TuplePoissonGenerator(val mean1: Double, mean2: Double) extends RandomDataGenerator[Vector] {
    val p1 = new PoissonGenerator(mean1)
    val p2 = new PoissonGenerator(mean2)
    override def nextValue(): Vector = {
      Vectors.dense(p1.nextValue(), p2.nextValue())
    }

    override def setSeed(seed: Long): Unit = {
      p1.setSeed(seed)
      p2.setSeed(seed)
    }

    override def copy(): RandomDataGenerator[Vector] = {
      new TuplePoissonGenerator(mean1, mean2)
    }
  }

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession
      .builder()
      .appName("MllibDataGen")
      .getOrCreate()

    sparkSession.sparkContext.getConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val rdd = RandomRDDs.randomRDD[Vector](
      sparkSession.sparkContext,
      new TuplePoissonGenerator(50, 20),
      100000l,
      30
    )

    rdd.zipWithIndex.map{case (v, index) =>
      (f"$index%06d", v.toArray(0).toString, v.toArray(1).toString)
    }.toHBaseTable("test:duck")
      .toColumns("x", "y")
      .inColumnFamily("bpoint")
      .save()

    val ardd = KMeansDataGenerator.generateKMeansRDD(sparkSession.sparkContext, 100000, 4, 2, 10, 30)
    val kmeanrdd = ardd.map{Vectors.dense(_)}

    kmeanrdd.zipWithIndex.map{case (v, index) =>
      (f"$index%06d", v.toArray(0).toString, v.toArray(1).toString)
    }.toHBaseTable("test:duck")
      .toColumns("x", "y")
      .inColumnFamily("kpoint")
      .save()

    //val clusters = KMeans.train(rdd, 2, 20)
    //val cluster2 = KMeans.train(kmeanrdd, 4, 20)

    //clusters.clusterCenters.foreach(println)

    while(true) {
      Thread.sleep(2)
    }
  }
}
