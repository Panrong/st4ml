package experiments

import instances.{Duration, Point, Trajectory}
import operatorsNew.selector.partitioner.STRPartitioner
import org.apache.spark.sql.SparkSession
import utils.Config

case class E(lon: Double, lat: Double, t: Long)

case class T(id: String, entries: Array[E])

case class TwP(id: String, entries: Array[E], pId: Int)

object WriteMetadataTest extends App {
  val fileName = args(0)
  val numPartitions = args(1).toInt
  val res = args(2)
  val metadata = args(3)

  val spark = SparkSession.builder()
    .appName("MetaDataTest")
    .master(Config.get("master"))
    .getOrCreate()

  val sc = spark.sparkContext
  sc.setLogLevel("ERROR")

  val readDs = spark.read.parquet(fileName)

  import spark.implicits._

  val trajRDD = readDs.as[T].rdd.map(x => {
    val entries = x.entries.map(p => (Point(p.lon, p.lat), Duration(p.t), None))
    Trajectory(entries, x.id)
  })
  println(trajRDD.count)
  val partitioner = new STRPartitioner(numPartitions, Some(0.2))
  val partitionedRDD = partitioner.partition(trajRDD).mapPartitionsWithIndex {
    case (idx, partition) => partition.map(x => (x, idx))
  }
  val trajDf = partitionedRDD.map(x => {
    val entries = x._1.entries.map(e => E(e.spatial.getX, e.spatial.getY, e.temporal.start))
    TwP(x._1.data, entries, x._2)
  }).toDF
  trajDf.write.partitionBy("pId").parquet(res)
  val partitionInfo = partitioner.partitionRange

  import java.io._

  val f = new File(metadata)
  f.createNewFile()
  val pw = new PrintWriter(f)
  for (x <- partitionInfo) {
    pw.write(s"${x._1} ${x._2.xMin} ${x._2.yMin} ${x._2.xMax} ${x._2.yMax}\n")
  }
  pw.close()

  sc.stop()
}
