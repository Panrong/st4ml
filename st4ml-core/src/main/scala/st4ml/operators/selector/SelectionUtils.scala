package st4ml.operators.selector

import st4ml.instances._
import st4ml.operators.selector.partitioner.STPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, exp}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK_SER
import org.locationtech.jts.geom.GeometryFactory
import org.locationtech.jts.io.WKTReader
import st4ml.utils.mapmatching.road.{RoadEdge, RoadGrid, RoadVertex}

import java.io.File
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.reflect.ClassTag

object SelectionUtils {
  type TrajDefault = Trajectory[None.type, Map[String, String]]
  type EventDefault = Event[Geometry, None.type, Map[String, String]]
  type PointEventDefault = Event[Point, None.type, Map[String, String]]

  case class PartitionInfo(pId: Long,
                           spatial: Array[Double],
                           temporal: Array[Long],
                           count: Long
                          )

  def granularitySetter[T <: Instance[_, _, _]](rdd: RDD[T], numPartitions: Int,
                                                sIndicator: String, tIndicator: String): (Int, Int) = {
    val tGranularity = sIndicator match {
      case "second" => 1
      case "minute" => 60
      case "hour" => 3600
      case "day" => 86400
      case "month" => 2592000
      case "season" => 7776000
      case "year" => 31536000
    }
    val sGranularity = tIndicator match {
      case "meter" => 1
      case "kilometer" => 1000
      case "km" => 1000
      case "meter_gps" => 1 / 111000
      case "km_gps" => 1 / 111
      case "kilometer_gps" => 1 / 111
      case "degree" => 1
    }
    val rangeRDD = rdd.sample(false, 0.1)
      .map(x => (x.extent.xMin, x.extent.xMax, x.extent.yMin, x.extent.yMax, x.duration.start, x.duration.end))
    val xMin = rangeRDD.map(_._1).min
    val xMax = rangeRDD.map(_._2).max
    val yMin = rangeRDD.map(_._3).min
    val yMax = rangeRDD.map(_._4).max
    val tMin = rangeRDD.map(_._5).min
    val tMax = rangeRDD.map(_._6).max

    val sWeight = (xMax - xMin) * (yMax - yMin) / sGranularity / sGranularity
    val tWeight = (tMax - tMin) / tGranularity
    val stRatio = sWeight / tWeight
    if (stRatio > numPartitions) (numPartitions, 1)
    if (stRatio < 1.0 / numPartitions) (1, numPartitions)
    else {
      val tNum = (numPartitions / (1 + stRatio)).toInt
      val sNum = (numPartitions / tNum)
      (sNum, tNum)
    }
  }

  implicit class InstanceWithIdFuncs[T <: Instance[_ <: Geometry, _, _] : ClassTag](rdd: RDD[(T, Int)]) {
    def calPartitionInfo: Array[(Int, Extent, Duration, Int)] = {
      //      rdd.persist(MEMORY_AND_DISK_SER)
      //      val xMinRdd = rdd.map(x => x._1.extent.xMin)
      //        .mapPartitionsWithIndex {
      //          case (id, iter) => Iterator((id, iter.min))
      //        }
      //      val yMinRdd = rdd.map(x => x._1.extent.yMin)
      //        .mapPartitionsWithIndex {
      //          case (id, iter) => Iterator((id, iter.min))
      //        }
      //      val xMaxRdd = rdd.map(x => x._1.extent.xMax)
      //        .mapPartitionsWithIndex {
      //          case (id, iter) => Iterator((id, iter.max))
      //        }
      //      val yMaxRdd = rdd.map(x => x._1.extent.yMax)
      //        .mapPartitionsWithIndex {
      //          case (id, iter) => Iterator((id, iter.max))
      //        }
      //      val tMinRdd = rdd.map(x => x._1.duration.start)
      //        .mapPartitionsWithIndex {
      //          case (id, iter) => Iterator((id, iter.min))
      //        }
      //      val tMaxRdd = rdd.map(x => x._1.duration.end)
      //        .mapPartitionsWithIndex {
      //          case (id, iter) => Iterator((id, iter.max))
      //        }
      //      val countRdd = rdd.mapPartitionsWithIndex { case (id, iter) => Iterator((id, iter.size)) }
      //
      //      val extentRdd = xMinRdd.join(yMinRdd).join(xMaxRdd).join(yMaxRdd).map(x => (x._1, Extent(x._2._1._1._1, x._2._1._1._2, x._2._1._2, x._2._2)))
      //      val tRdd = tMinRdd.join(tMaxRdd).map(x => (x._1, Duration(x._2._1, x._2._2)))
      //      val resRdd = extentRdd.join(tRdd).join(countRdd).map(x => (x._1, x._2._1._1, x._2._1._2, x._2._2))
      //      resRdd.collect()
      // the method above takes too much memory
      rdd.mapPartitionsWithIndex {
        case (id, iter) =>
          var xMin = 180.0
          var yMin = 90.0
          var xMax = -180.0
          var yMax = -90.0
          var tMin = 10000000000L
          var tMax = 0L
          var count = 0
          while (iter.hasNext) {
            val next = iter.next()._1
            val mbr = next.extent
            if (mbr.xMin < xMin) xMin = mbr.xMin
            if (mbr.xMax > xMax) xMax = mbr.xMax
            if (mbr.yMin < yMin) yMin = mbr.yMin
            if (mbr.yMax > yMax) yMax = mbr.yMax
            if (next.duration.start < tMin) tMin = next.duration.start
            if (next.duration.end > tMax) tMax = next.duration.end
            count += 1
          }
          if (count == 0) Iterator(None)
          else Iterator(Some((id, new Extent(xMin, yMin, xMax, yMax), Duration(tMin, tMax), count)))
      }.filter(_.isDefined).map(_.get)
        .collect
    }
  }

  implicit class InstanceFuncs[T <: Instance[_ <: Geometry, _, _] : ClassTag](rdd: RDD[T]) {
    def stPartition[P <: STPartitioner : ClassTag](partitioner: P): RDD[T] = partitioner.partition(rdd)

    def stPartitionWithInfo[P <: STPartitioner : ClassTag](partitioner: P,
                                                           duplicate: Boolean = false): (RDD[(T, Int)], Array[PartitionInfo]) = {
      val partitionedRDD = if (duplicate) partitioner.partitionWDup(rdd)
      else partitioner.partition(rdd)
      val pRDD = partitionedRDD.mapPartitionsWithIndex {
        case (idx, partition) => partition.map(x => (x, idx))
      }
      val pInfo = pRDD.calPartitionInfo.map(x =>
        PartitionInfo(x._1, Array(x._2.xMin, x._2.yMin, x._2.xMax, x._2.yMax), Array(x._3.start, x._3.end), x._4)
      )
      (pRDD, pInfo)
    }
  }

  object LoadPartitionInfo {
    def apply(dir: String): RDD[(Long, Extent, Duration, Long)] = {
      val spark: SparkSession = SparkSession.builder.getOrCreate()
      import spark.implicits._
      val metadataDs = spark.read.json(dir).as[PartitionInfo]
      metadataDs.rdd.map(x => (x.pId,
        Extent(x.spatial(0), x.spatial(1), x.spatial(2), x.spatial(3)), Duration(x.temporal(0),
        x.temporal(1)),
        x.count)
      )
    }
  }

  object LoadPartitionInfoLocal {
    def apply(dir: String): Array[(Long, Extent, Duration, Long)] = {
      import scala.util.parsing.json._
      val f = scala.io.Source.fromFile(dir)
      f.getLines.map(line => {
        val map = JSON.parseFull(line).get.asInstanceOf[Map[String, Any]]
        val pId = map("pId").asInstanceOf[Double].toLong
        val coordinates = map("spatial").asInstanceOf[List[Double]]
        val spatial = new Extent(coordinates.head, coordinates(1), coordinates(2), coordinates(3))
        val t = map("temporal").asInstanceOf[List[Double]].map(_.toLong)
        val temporal = new Duration(t.head, t(1))
        val count = map("count").asInstanceOf[Double].toLong
        (pId, spatial, temporal, count)
      }).toArray
    }
  }

  implicit class PartitionInfoFunc(pInfo: Array[PartitionInfo]) {
    def toDisk(metadataDir: String): Unit = {
      val spark: SparkSession = SparkSession.builder.getOrCreate()
      val pInfoDf = spark.createDataFrame(pInfo)
      pInfoDf.coalesce(1).write.json(metadataDir)
    }
  }

  /** case classes for persisting ST st4ml.instances */
  //  case class E(lon: Double, lat: Double, t: Long, v: String, d: String) // event
  //
  //  case class EwP(lon: Double, lat: Double, t: Long, pId: Int) // event with partition ID
  //
  //  case class T(id: String, entries: Array[E]) // trajectory
  //
  //  case class TwP(id: String, entries: Array[E], pId: Int) // trajectory with partition ID
  case class E(shape: String, timeStamp: Array[Long], v: Option[String], d: String)

  case class EwP(shape: String, timeStamp: Array[Long], v: Option[String], d: String, pId: Int)

  case class TrajPoint(lon: Double, lat: Double, t: Array[Long], v: Option[String])

  case class T(points: Array[TrajPoint], d: String)

  case class TwP(points: Array[TrajPoint], d: String, pId: Int)

  // for parsing csv

  case class PreE(shape: String, t: Array[String], data: Map[String, String])

  case class PreT(shape: String, timestamps: String, data: Map[String, String])

  /** rdd2Df conversion functions */
  //  trait Ss {
  //    val spark: SparkSession = SparkSession.builder.getOrCreate()
  //  }


  implicit class EventRDDFunc[S <: Geometry : ClassTag, V: ClassTag, D: ClassTag](rdd: RDD[Event[S, V, D]]) extends Serializable {
    val spark: SparkSession = SparkSession.builder.getOrCreate()

    import spark.implicits._

    def toDs(vFunc: V => Option[String] = x => if (x.isInstanceOf[None.type]) None else Some(x.toString),
             dFunc: D => String = _.toString): Dataset[E] = {
      rdd.map(event => {
        val entry = event.entries.head
        val timeStamp = Array(entry.temporal.start, entry.temporal.end)
        val v = vFunc(entry.value)
        val d = dFunc(event.data)
        val shape = entry.spatial.toString
        E(shape, timeStamp, v, d)
      }).toDS
    }

    def perPartitionIndex: RDD[RTree[S, Event[S, V, D]]] = {
      rdd.mapPartitions { partition =>
        val arr = partition.toArray
        val capacity = math.sqrt(arr.length).toInt
        val geomArr = arr.map(_.entries.head.spatial)
        val durArr = arr.map(_.duration)
        var entries = new Array[(S, Event[S, V, D], Int)](0)
        for (i <- arr.indices) {
          geomArr(i).setUserData(Array(durArr(i).start.toDouble, durArr(i).end.toDouble))
          entries = entries :+ (geomArr(i).copy.asInstanceOf[S], arr(i), i)
        }
        Iterator(RTree[S, Event[S, V, D]](entries, capacity, dimension = 3))
      }
    }
  }

  implicit class PEventRDDFuncs[S <: Geometry : ClassTag, V: ClassTag, D: ClassTag](rdd: RDD[(Event[S, V, D], Int)]) {
    val spark: SparkSession = SparkSession.builder.getOrCreate()

    import spark.implicits._

    def toDs(vFunc: V => Option[String] = x => if (x.isInstanceOf[None.type]) None else Some(x.toString),
             dFunc: D => String = _.toString): Dataset[EwP] = {
      rdd.map { case (event, pId) =>
        val entry = event.entries.head
        val timeStamp = Array(entry.temporal.start, entry.temporal.end)
        val v = vFunc(entry.value)
        val d = dFunc(event.data)
        val shape = entry.spatial.toString
        EwP(shape, timeStamp, v, d, pId)
      }
    }.toDS

    def toDisk(dataDir: String,
               vFunc: V => Option[String] = x => if (x.isInstanceOf[None.type]) None else Some(x.toString),
               dFunc: D => String = _.toString): Unit = this.toDs(vFunc, dFunc).toDisk(dataDir)
  }

  implicit class TrajRDDFuncs[V: ClassTag, D: ClassTag](rdd: RDD[Trajectory[V, D]]) {
    val spark: SparkSession = SparkSession.builder.getOrCreate()

    import spark.implicits._

    def toDs(vFunc: V => Option[String] = x => if (x.isInstanceOf[None.type]) None else Some(x.toString),
             dFunc: D => String = x => x.toString): Dataset[T] = {
      rdd.map(traj => {
        val points = traj.entries.map(e => TrajPoint(e.spatial.getX, e.spatial.getY, Array(e.temporal.start, e.temporal.end), vFunc(e.value)))
        val d = dFunc(traj.data)
        T(points, d)
      }).toDS
    }

    def perPartitionIndex: RDD[RTree[Polygon, Trajectory[V, D]]] = {
      rdd.mapPartitions { partition =>
        val arr = partition.toArray
        val capacity = math.sqrt(arr.length).toInt
        val geomArr = arr.map(_.extent.toPolygon)
        val durArr = arr.map(_.duration)
        var entries = new Array[(Polygon, Trajectory[V, D], Int)](0)
        for (i <- arr.indices) {
          geomArr(i).setUserData(Array(durArr(i).start.toDouble, durArr(i).end.toDouble))
          entries = entries :+ (geomArr(i).copy.asInstanceOf[Polygon], arr(i), i)
        }
        Iterator(RTree[Polygon, Trajectory[V, D]](entries, capacity, dimension = 3))
      }
    }

  }

  implicit class PTrajRDDFuncs[V: ClassTag, D: ClassTag](rdd: RDD[(Trajectory[V, D], Int)]) {
    // partitioned traj
    val spark: SparkSession = SparkSession.builder.getOrCreate()

    import spark.implicits._

    def toDs(vFunc: V => Option[String] = x => if (x.isInstanceOf[None.type]) None else Some(x.toString),
             dFunc: D => String = x => x.toString): Dataset[TwP] = {
      rdd.map { case (traj, pId) =>
        val points = traj.entries.map(e => TrajPoint(e.spatial.getX, e.spatial.getY, Array(e.temporal.start, e.temporal.end), vFunc(e.value)))
        val d = dFunc(traj.data)
        TwP(points, d, pId)

      }.toDS
    }

    def toDisk(dataDir: String,
               vFunc: V => Option[String] = x => if (x.isInstanceOf[None.type]) None else Some(x.toString),
               dFunc: D => String = x => x.toString): Unit = this.toDs(vFunc, dFunc).toDisk(dataDir)
  }

  implicit class TrajDsFuncs(ds: Dataset[T]) {
    def toRdd: RDD[Trajectory[Option[String], String]] = {
      ds.rdd.map(traj => {
        val data = traj.d
        val entries = traj.points.map(point => {
          val s = Point(point.lon, point.lat)
          val t = Duration(point.t(0), point.t(1))
          val v = point.v
          (s, t, v)
        })
        Trajectory(entries, data)
      })
    }

    def toDisk(dataDir: String): Unit = {
      ds.toDF.write.parquet(dataDir)
    }
  }

  implicit class PTrajDsFuncs(ds: Dataset[TwP]) {
    def toRdd: RDD[(Trajectory[Option[String], String], Int)] = {
      ds.rdd.map(trajWId => {
        val data = trajWId.d
        val entries = trajWId.points.map(point => {
          val s = Point(point.lon, point.lat)
          val t = Duration(point.t(0), point.t(1))
          val v = point.v
          (s, t, v)
        })
        val pId = trajWId.pId
        (Trajectory(entries, data), pId)
      })
    }

    def toDisk(dataDir: String): Unit = {
      ds.toDF.write.partitionBy("pId").parquet(dataDir)
    }
  }

  implicit class EventDsFuncs(ds: Dataset[E]) {
    def toRdd: RDD[Event[Geometry, Option[String], String]] = {
      ds.rdd.map(x => {
        val shape = x.shape
        val s = String2Geometry(shape)
        val t = Duration(x.timeStamp(0), x.timeStamp(1))
        val v = x.v
        val d = x.d
        Event(s, t, v, d)
      })
    }
  }

  implicit class PEventDsFuncs(ds: Dataset[EwP]) {
    def toRdd: RDD[(Event[Geometry, Option[String], String], Int)] = {
      ds.rdd.map(x => {
        val shape = x.shape
        val s = String2Geometry(shape)
        val t = Duration(x.timeStamp(0), x.timeStamp(1))
        val v = x.v
        val d = x.d
        val pId = x.pId
        (Event(s, t, v, d), pId)
      })
    }

    def toDisk(dataDir: String): Unit = {
      ds.toDF.write
        .partitionBy("pId").parquet(dataDir)
    }
  }

  object String2Geometry {
    def apply(shape: String): Geometry = {
      val t = shape.split(" ").head
      t match {
        case "POINT" =>
          val content = ("""\([^]]+\)""".r findAllIn shape).next.drop(1).dropRight(1).split(" ").map(_.toDouble)
          Point(content(0), content(1))
        case "LINESTRING" =>
          val content = ("""\([^]]+\)""".r findAllIn shape).next.drop(1).dropRight(1).split(", ")
            .map(x => x.split(" ").map(_.toDouble))
          val points = content.map(x => Point(x(0), x(1)))
          LineString(points)
        case "POLYGON" =>
          val content = ("""\([^]]+\)""".r findAllIn shape).next.drop(2).dropRight(2).split(", ")
            .map(x => x.split(" ").map(_.toDouble))
          val points = content.map(x => Point(x(0), x(1)))
          Polygon(points)
        case _ => throw new ClassCastException("Unknown shape Only point, linestring and polygon are supported.")
      }
    }
  }

  def readMap(mapDir: String): Array[Polygon] = {
    val spark = SparkSession.getActiveSession.get
    val map = spark.read.option("delimiter", " ").csv(mapDir).rdd
    val mapRDD = map.map(x => {
      val points = x.getString(1).drop(1).dropRight(1).split("\\),").map { x =>
        val p = x.replace("(", "").replace(")", "").split(",").map(_.toDouble)
        Point(p(0), p(1))
      }
      LineString(points)
    })
    mapRDD.collect.map(x => Polygon(x.getCoordinates ++ x.getCoordinates.reverse))
  }

  def readPOI(poiDir: String = "../poi_small.json"): RDD[Event[Point, None.type, String]] = {
    val spark = SparkSession.getActiveSession.get
    val poiDf = spark.read.json(poiDir)
      .select("g", "id").filter(col("g").isNotNull && col("id").isNotNull)
    poiDf.rdd.mapPartitions { x =>
      val wktReader = new WKTReader()
      x.map { p =>
        val id = p.getLong(1).toString
        val point = wktReader.read(p.getString(0)).asInstanceOf[Point]
        Event(point, Duration.empty, d = id)
      }
    }
  }

  def readArea(areaDir: String = "../postal_small.json"): Array[(Long, Polygon)] = {
    val spark = SparkSession.getActiveSession.get
    val areaDf = spark.read.json(areaDir).select("g", "id").filter(col("g").isNotNull && col("id").isNotNull &&
      !col("g").startsWith("POINT") && !col("g").startsWith("LINESTRING"))
    val areaRdd = areaDf.rdd.map { x =>
      val wktReader = new WKTReader()
      val p = wktReader.read(x.getString(0))
      val gf = new GeometryFactory()
      var polygon: Option[Polygon] = None
      try {
        polygon = Some(gf.createPolygon(p.getEnvelope.getCoordinates))
      }
      catch {
        case _: Exception => println(p)
      }
      (x.getLong(1), polygon)
    }.filter(_._2.isDefined).map(x => (x._1, x._2.get))
    areaRdd.collect()
  }

  def ReadRaster(dir: String): (Array[Polygon], Array[Duration]) = {
    val spark = SparkSession.getActiveSession.get
    val df = spark.read.csv(dir)
    val rdd = df.rdd.map { x =>
      val polygon = Extent(x.getString(0).toDouble, x.getString(1).toDouble,
        x.getString(2).toDouble, x.getString(3).toDouble).toPolygon
      val duration = Duration(x.getString(4).toLong, x.getString(5).toLong)
      (polygon, duration)
    }
    (rdd.map(_._1).collect, rdd.map(_._2).collect)
  }

  def readOsm(mapDir: String, gridSize: Double = 0.1): RoadGrid = {
    val spark = SparkSession.getActiveSession.get
    val edgesDir = mapDir.stripMargin('/') + "/edges.csv"
    val edgeDf = spark.read.option("header", true).csv(edgesDir)
    val edges = edgeDf.rdd.map { edge =>
      val wktReader = new WKTReader()
      val linestring = wktReader.read(edge.getString(0)).asInstanceOf[LineString]
      val from = edge.getString(1)
      val to = edge.getString(2)
      val id = edge.getString(3)
      val length = edge.getString(5).toDouble
      RoadEdge(s"$from-$to", from, to, length, linestring)
    }.collect

    val nodesDir = mapDir.stripMargin('/') + "/nodes.csv"
    val nodeDf = spark.read.option("header", "true").csv(nodesDir)
    val nodes = nodeDf.rdd.map { node =>
      val wktReader = new WKTReader()
      val point = wktReader.read(node.getString(1)).asInstanceOf[Point]
      val id = node.getString(0)
      RoadVertex(id, point)
    }.collect
    val minLon: Double = List(
      nodes.map(_.point.getX).min,
      edges.flatMap(_.ls.getCoordinates.map(_.x)).min
    ).min
    val minLat: Double = List(
      nodes.map(_.point.getY).min,
      edges.flatMap(_.ls.getCoordinates.map(_.y)).min
    ).min
    val maxLon: Double = List(
      nodes.map(_.point.getX).max,
      edges.flatMap(_.ls.getCoordinates.map(_.x)).max
    ).max
    val maxLat: Double = List(
      nodes.map(_.point.getY).max,
      edges.flatMap(_.ls.getCoordinates.map(_.y)).max
    ).max
    new RoadGrid(nodes, edges, minLon, minLat, maxLon, maxLat, gridSize)
  }
}
