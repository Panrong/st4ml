package st4ml.operators.selector

import st4ml.instances._
import st4ml.operators.selector.partitioner.STPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.collection.mutable
import scala.reflect.ClassTag

object SelectionUtils {

  case class PartitionInfo(pId: Long,
                           spatial: Array[Double],
                           temporal: Array[Long],
                           count: Long
                          )

  implicit class InstanceWithIdFuncs[T <: Instance[_ <: Geometry, _, _] : ClassTag](rdd: RDD[(T, Int)]) {
    def calPartitionInfo: Array[(Int, Extent, Duration, Int)] = {
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

  /** rdd2Df conversion functions */
  //  trait Ss {
  //    val spark: SparkSession = SparkSession.builder.getOrCreate()
  //  }


  implicit class EventRDDFunc[S <: Geometry : ClassTag, V: ClassTag, D: ClassTag](rdd: RDD[Event[S, V, D]]) {
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
               dFunc: D => String = _.toString,
               maxRecords: Int = 10000): Unit = this.toDs(vFunc, dFunc).toDisk(dataDir, maxRecords)
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
               dFunc: D => String = x => x.toString,
               maxRecords: Int = 10000): Unit = this.toDs(vFunc, dFunc).toDisk(dataDir, maxRecords)
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

    def toDisk(dataDir: String, maxRecords: Int = 10000): Unit = {
      ds.toDF.write
        .option("maxRecordsPerFile", maxRecords)
        .parquet(dataDir)
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

    def toDisk(dataDir: String, maxRecords: Int = 10000): Unit = {
      ds.toDF.write
        .option("maxRecordsPerFile", maxRecords)
        .partitionBy("pId").parquet(dataDir)
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

    def toDisk(dataDir: String, maxRecords: Int = 10000): Unit = {
      ds.toDF.write
        .option("maxRecordsPerFile", maxRecords)
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

}
