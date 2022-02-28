package experiments

import st4ml.instances.{Duration, Event, Extent, Point, Polygon, LineString}
import st4ml.operators.selector.SelectionUtils._
import st4ml.operators.selector.partitioner._
import org.apache.spark.sql.SparkSession
import st4ml.utils.Config

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
  val metadata = args(3)
  val m = args(4)
  val stRatio = args(5).toDouble
  val sNumPartitions = math.sqrt(numPartitions / stRatio).toInt
  val tNumPartitions = numPartitions / sNumPartitions
  val spark = SparkSession.builder()
    .appName("MetaDataTest")
    .master(Config.get("master"))
    .getOrCreate()

  val sc = spark.sparkContext
  sc.setLogLevel("ERROR")

  import spark.implicits._

  //  val trajRDD = readDs.as[T].rdd.map(x => {
  //    val entries = x.entries.map(p => (Point(p.lon, p.lat), Duration(p.t), None))
  //    Trajectory(entries, x.id)
  //  })
  //  println(trajRDD.take(5).deep)
  //  trajRDD.toDs().write.parquet("datasets/traj_example_parquet_customized")

  if (m == "traj") {

    /** TEST TRAJ */
    /** read trajectory dataset of ST-Tool format */
    val trajDs = spark.read.parquet(fileName).as[T]
    val trajRDD = trajDs.toRdd
    println(trajRDD.count)
    /** partition trajRDD and persist on disk */
    val partitioner = new TSTRPartitioner(sNumPartitions, tNumPartitions, Some(1))
    val (partitionedRDDWithPId, pInfo) = trajRDD.stPartitionWithInfo(partitioner)
    //    pInfo.foreach(println)
    val trajDsWithPid = partitionedRDDWithPId.toDs()
    trajDsWithPid.show(2, truncate = false)
    pInfo.toDisk(metadata)
    trajDsWithPid.toDisk(res)

    /** END TEST TRAJ */
  }
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
  else if (m == "event") {
    /** TEST EVENT */
    /** read event dataset of ST-Tool format */
    val eventDs = spark.read.parquet(fileName).as[E]
    val eventRDD = eventDs.toRdd
    eventRDD.take(2).foreach(println)
    println(eventRDD.count)
    /** partition trajRDD and persist on disk */
    val partitioner = TSTRPartitioner(numPartitions, Some(0.2))
    val (partitionedRDDWithPId, pInfo) = eventRDD.stPartitionWithInfo(partitioner)
    val EventDsWithPid = partitionedRDDWithPId.toDs()
    EventDsWithPid.show(2, truncate = false)
    pInfo.toDisk(metadata)
    partitionedRDDWithPId.toDisk(res)

    /** END TEST EVENT */
  }
  sc.stop()
}
