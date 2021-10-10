package experiments

import instances.{Duration, Event, Extent, Point, Polygon, LineString}
import operatorsNew.selector.SelectionUtils._
import operatorsNew.selector.partitioner._
import org.apache.spark.sql.SparkSession
import utils.Config

//
//case class E(id: String, lon: Double, lat: Double, t: Long) // event
//
//case class EwP(lon: Double, lat: Double, t: Long, pId: Int) // event with partition ID
//
//case class T(id: String, entries: Array[E]) // trajectory
//
//case class TwP(id: String, entries: Array[E], pId: Int) // trajectory with partition ID

object WriteMetadataTest extends App {
  val fileName = args(0)
  val numPartitions = args(1).toInt
  val res = args(2)
  //  val metadata = args(3)

  val spark = SparkSession.builder()
    .appName("MetaDataTest")
    .master(Config.get("master"))
    .getOrCreate()

  val sc = spark.sparkContext
  sc.setLogLevel("ERROR")

  val readDs = spark.read.parquet(fileName)

  import spark.implicits._

  //  val trajRDD = readDs.as[T].rdd.map(x => {
  //    val entries = x.entries.map(p => (Point(p.lon, p.lat), Duration(p.t), None))
  //    Trajectory(entries, x.id)
  //  })
  //  println(trajRDD.take(5).deep)
  //  trajRDD.toDs().write.parquet("datasets/traj_example_parquet_customized")

  //  /** read trajectory dataset of ST-Tool format */
  //  val trajDs = spark.read.parquet("datasets/traj_example_parquet_customized").as[T]
  //  val trajRDD = trajDs.toRdd
  //  println(trajRDD.count)
  //
  //  /** partition trajRDD and persist on disk */
  //  val partitioner = new STRPartitioner(numPartitions, Some(0.2))
  //  val (partitionedRDDWithPId, pInfo) = trajRDD.stPartitionWithInfo(partitioner)
  //  val trajDsWithPid = partitionedRDDWithPId.toDs()
  //  trajDsWithPid.show(5, truncate = false)
  //  pInfo.toDisk("datasets/tmpMetadata")
  ////  trajDsWithPid.toDisk("datasets/persisted_traj")
  //
  //  /** load persisted partitioned trajRDD and metadata */
  //  /** usually not used since this is to load everything */
  //  val pTrajRDD = spark.read.parquet("datasets/persisted_traj")
  //    .as[TwP]
  //    .toRdd
  //  pTrajRDD.take(5).foreach(println)
  //
  //  val metadata = LoadMetadata("datasets/tmpMetadata")
  //  println(metadata.deep)

  //  val eventRdd = spark.read.parquet("datasets/porto_points").as[E].rdd.map(x =>
  //    Event(Point(x.lon, x.lat), Duration(x.t), d = x.id)
  //  )
  //  case class newE(shape: String, timeStamp: Array[Long], v: Option[String], d: String)
  //  val eventDs = eventRdd.map(event => newE(event.entries.head.spatial.toString,
  //    Array(event.entries.head.temporal.start, event.entries.head.temporal.end),
  //    event.entries.head.value, event.data)).toDS()
  //  eventDs.write.parquet("event_example_parquet_customized")

  /** read event dataset of ST-Tool format */
  val eventDs = spark.read.parquet("datasets/event_example_parquet_customized").as[E]
  val eventRDD = eventDs.toRdd
  eventRDD.take(5).foreach(println)
  println(eventRDD.count)
  /** partition trajRDD and persist on disk */
  val partitioner = new STRPartitioner(numPartitions, Some(0.2))
  val (partitionedRDDWithPId, pInfo) = eventRDD.stPartitionWithInfo(partitioner)
  println(pInfo.deep)
  val EventDsWithPid = partitionedRDDWithPId.toDs()
  EventDsWithPid.show(5, truncate = false)
  pInfo.toDisk("datasets/eventMetadata")
  EventDsWithPid.toDisk("datasets/persisted_event")

  sc.stop()
}