package st4ml.operators.converter

import st4ml.instances._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.SizeEstimator
import st4ml.instances.RoadNetwork._

import scala.reflect.ClassTag

class Traj2SpatialMapConverter(sArray: Array[Polygon],
                               override val optimization: String = "rtree") extends Converter {

  val sMap: Array[(Int, Polygon)] = sArray.sortBy(x =>
    (x.getCoordinates.map(c => c.x).min, x.getCoordinates.map(c => c.y).min)).zipWithIndex.map(_.swap)
  var rTree: Option[RTree[Polygon, String]] = None

  lazy val smXMin: Double = sMap.head._2.getEnvelopeInternal.getMinX
  lazy val smYMin: Double = sMap.head._2.getEnvelopeInternal.getMinY
  lazy val smXMax: Double = sMap.last._2.getEnvelopeInternal.getMaxX
  lazy val smYMax: Double = sMap.last._2.getEnvelopeInternal.getMaxY
  lazy val smXLength: Double = sMap.head._2.getEnvelopeInternal.getMaxX - smXMin
  lazy val smYLength: Double = sMap.head._2.getEnvelopeInternal.getMaxY - smYMin
  lazy val smXSlots: Long = ((smXMax - smXMin) / smXLength).round
  lazy val smYSlots: Long = ((smYMax - smYMin) / smYLength).round

  def buildRTree(spatials: Array[Polygon]): RTree[Polygon, String] = {
    val r = math.min(math.sqrt(spatials.length).toInt, 8)
    val entries = spatials.zipWithIndex.map(x => (x._1, x._2.toString, x._2))
    RTree[Polygon, String](entries, r)
  }

  def convert[V: ClassTag, D: ClassTag](input: RDD[Trajectory[V, D]]): RDD[SpatialMap[Polygon, Array[Trajectory[V, D]], None.type]] = {
    type I = Trajectory[V, D]
    type O = SpatialMap[Polygon, Array[I], None.type]
    if (optimization == "none") {
      input.mapPartitions(partition => {
        val trajs = partition.toArray
        val emptySm = SpatialMap.empty[Polygon, I](sArray)
        Iterator(emptySm.attachInstance(trajs, trajs.map(_.toGeometry)))
      })
    }
    else if (optimization == "rtree") {
      rTree = Some(buildRTree(sMap.map(_._2)))
      val spark = SparkSession.builder().getOrCreate()
      val rTreeBc = spark.sparkContext.broadcast(rTree)
      input.mapPartitions(partition => {
        val trajs = partition.toArray
        val emptySm = SpatialMap.empty[Polygon, I](sArray)
        emptySm.rTree = rTreeBc.value
        Iterator(emptySm.attachInstanceRTree(trajs, trajs.map(_.toGeometry)))
      })
    }
    else if (optimization == "regular") {
      val emptySm = SpatialMap.empty[Polygon, I](sArray)
      //assert(emptySm.isRegular, "The structure is not regular.")
      input.flatMap(e => {
        val xMin = e.extent.xMin
        val xMax = e.extent.xMax
        val yMin = e.extent.yMin
        val yMax = e.extent.yMax
        val xRanges = Range(math.max(0, ((xMin - smXMin) / smXLength).toInt), math.min(smXSlots.toInt - 1, ((xMax - smXMin) / smXLength).toInt)).toArray
        val yRanges = Range(math.max(0, ((yMin - smYMin) / smYLength).toInt), math.min(smXSlots.toInt - 1, ((yMax - smYMin) / smYLength).toInt)).toArray
        var idRanges = new Array[Int](0)
        for (i <- xRanges; j <- yRanges) idRanges = idRanges :+ i * smYSlots.toInt + j
        idRanges = idRanges.filter(x => e.intersects(sMap(x)._2))
        idRanges.map(x => (e, x))
      })
        .mapPartitions(partition => {
          val trajs = partition.toArray.groupBy(_._2).mapValues(x => x.map(_._1))
          val emptySm = SpatialMap.empty[Polygon, I](sArray).sorted
          Iterator(emptySm.createSpatialMap(trajs))
        })
    }
    else if (optimization == "rtree2") {
      input.mapPartitions(partition => {
        val trajs = partition.toArray.map(x => (x.extent.toPolygon, x, 0))
        val rTree = RTree[Polygon, I](trajs, math.min(math.sqrt(trajs.length).toInt, 128), 2)
        val entries = sArray.map(x => new Entry(x,Duration.empty, rTree.range(x).map(_._2)))
        Iterator(SpatialMap(entries))
      })
    }
    else throw new NoSuchElementException
  }

  def convert[V: ClassTag, D: ClassTag,
    V2: ClassTag, D2: ClassTag](input: RDD[Trajectory[V, D]], agg: Array[Trajectory[V, D]] => V2): RDD[SpatialMap[Polygon, V2, None.type]] = {
    type I = Trajectory[V, D]
    type O = SpatialMap[Polygon, V2, None.type]
    if (optimization == "none") {
      input.mapPartitions(partition => {
        val trajs = partition.toArray
        val emptySm = SpatialMap.empty[Polygon, I](sArray)
        Iterator(emptySm.attachInstance(trajs, trajs.map(_.toGeometry)).mapValue(agg))
      })
    }
    else if (optimization == "rtree") {
      rTree = Some(buildRTree(sMap.map(_._2)))
      val spark = SparkSession.builder().getOrCreate()
      val rTreeBc = spark.sparkContext.broadcast(rTree)
      input.mapPartitions(partition => {
        val trajs = partition.toArray
        val emptySm = SpatialMap.empty[Polygon, I](sArray)
        emptySm.rTree = rTreeBc.value
        Iterator(emptySm.attachInstanceRTree(trajs, trajs.map(_.toGeometry)).mapValue(agg))
      })
    }
    else if (optimization == "regular") {
      val emptySm = SpatialMap.empty[Polygon, I](sArray)
      //assert(emptySm.isRegular, "The structure is not regular.")
      input.flatMap(e => {
        val xMin = e.extent.xMin
        val xMax = e.extent.xMax
        val yMin = e.extent.yMin
        val yMax = e.extent.yMax
        val xRanges = Range(math.max(0, ((xMin - smXMin) / smXLength).toInt), math.min(smXSlots.toInt - 1, ((xMax - smXMin) / smXLength).toInt)).toArray
        val yRanges = Range(math.max(0, ((yMin - smYMin) / smYLength).toInt), math.min(smXSlots.toInt - 1, ((yMax - smYMin) / smYLength).toInt)).toArray
        var idRanges = new Array[Int](0)
        for (i <- xRanges; j <- yRanges) idRanges = idRanges :+ i * smYSlots.toInt + j
        idRanges = idRanges.filter(x => e.intersects(sMap(x)._2))
        idRanges.map(x => (e, x))
      })
        .mapPartitions(partition => {
          val trajs = partition.toArray.groupBy(_._2).mapValues(x => x.map(_._1))
          val emptySm = SpatialMap.empty[Polygon, I](sArray).sorted
          Iterator(emptySm.createSpatialMap(trajs).mapValue(agg))
        })
    }
    else throw new NoSuchElementException
  }

  def convert[D: ClassTag, V2: ClassTag](input: RDD[Trajectory[String, D]],
                                         roadNetwork: RoadNetwork):
  RDD[SpatialMap[LineString, (String, Array[Trajectory[String, D]]), None.type]] = {
    val sMap = roadNetwork.entries.map(e => (e.value, e.spatial)).toMap
    val emptyMap = sMap.map(x => (x._2, (x._1, new Array[Trajectory[String, D]](0))))
    input.mapPartitions(partition => {
      val trajs = partition.toArray
      val values = trajs.flatMap(traj => traj.entries.map(e => (e.value, traj))).groupBy(_._1).mapValues(_.map(_._2)).toArray
        .filter(x => sMap.contains(x._1))
        .map(x => (sMap(x._1), x)).toMap
      val allocatedMap = (emptyMap ++ values).toArray
      val entries = allocatedMap.map(x => Entry(x._1, Duration.empty, x._2))
      Iterator(SpatialMap(entries, None))
    })
  }

  def convert[V: ClassTag, D: ClassTag,
    V2: ClassTag, D2: ClassTag,
    V3: ClassTag](input: RDD[Trajectory[V, D]], preMap: Trajectory[V, D] => Trajectory[V2, D2],
                  agg: Array[Trajectory[V2, D2]] => V3):
  RDD[SpatialMap[Polygon, V3, None.type]] = {
    type I = Trajectory[V2, D2]
    type O = SpatialMap[Polygon, V3, None.type]
    if (optimization == "none") {
      input.map(preMap).mapPartitions(partition => {
        val trajs = partition.toArray
        val emptySm = SpatialMap.empty[Polygon, I](sArray)
        Iterator(emptySm.attachInstance(trajs, trajs.map(_.toGeometry)).mapValue(agg))
      })
    }
    else if (optimization == "rtree") {
      rTree = Some(buildRTree(sMap.map(_._2)))
      val spark = SparkSession.builder().getOrCreate()
      val rTreeBc = spark.sparkContext.broadcast(rTree)
      input.map(preMap).mapPartitions(partition => {
        val trajs = partition.toArray
        val emptySm = SpatialMap.empty[Polygon, I](sArray)
        emptySm.rTree = rTreeBc.value
        Iterator(emptySm.attachInstanceRTree(trajs, trajs.map(_.toGeometry)).mapValue(agg))
      })
    }
    else if (optimization == "regular") {
      val emptySm = SpatialMap.empty[Polygon, I](sArray)
      //assert(emptySm.isRegular, "The structure is not regular.")
      input.map(preMap).flatMap(e => {
        val xMin = e.extent.xMin
        val xMax = e.extent.xMax
        val yMin = e.extent.yMin
        val yMax = e.extent.yMax
        val xRanges = Range(math.max(0, ((xMin - smXMin) / smXLength).toInt), math.min(smXSlots.toInt - 1, ((xMax - smXMin) / smXLength).toInt)).toArray
        val yRanges = Range(math.max(0, ((yMin - smYMin) / smYLength).toInt), math.min(smXSlots.toInt - 1, ((yMax - smYMin) / smYLength).toInt)).toArray
        var idRanges = new Array[Int](0)
        for (i <- xRanges; j <- yRanges) idRanges = idRanges :+ i * smYSlots.toInt + j
        idRanges = idRanges.filter(x => e.intersects(sMap(x)._2))
        idRanges.map(x => (e, x))
      })
        .mapPartitions(partition => {
          val trajs = partition.toArray.groupBy(_._2).mapValues(x => x.map(_._1))
          val emptySm = SpatialMap.empty[Polygon, I](sArray).sorted
          Iterator(emptySm.createSpatialMap(trajs).mapValue(agg))
        })
    }
    else throw new NoSuchElementException
  }
}

object Traj2SpatialMapConverterTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").getOrCreate()
    val sc = spark.sparkContext

    val trajs = Array(
      Trajectory(Array(Point(-2, 4), Point(1, 7)), Array(Duration(34), Duration(94)), Array(None, None), "0"),
      Trajectory(Array(Point(5, 6), Point(7, 8)), Array(Duration(134), Duration(274)), Array(None, None), "1"),
      Trajectory(Array(Point(9, 10), Point(11, 12)), Array(Duration(234), Duration(284)), Array(None, None), "2"),
      Trajectory(Array(Point(13, 14), Point(15, 16)), Array(Duration(334), Duration(364)), Array(None, None), "3")
    )

    val eventRDD = sc.parallelize(trajs)

    val sArray = Array(
      Extent(0, 0, 5, 5).toPolygon, // 0
      Extent(2, 2, 8, 8).toPolygon, // 1
      Extent(5, 5, 10, 10).toPolygon, // 2
      Extent(10, 10, 20, 20).toPolygon // 2
    )

    val f: Array[Trajectory[None.type, String]] => Int = _.length

    //    val f: Array[Trajectory[None.type, String]] => Array[Trajectory[None.type, String]] = x => x
    val countConverter = new Traj2SpatialMapConverter(sArray,optimization = "rtree")

    val tsRDD = countConverter.convert(eventRDD, f)
    tsRDD.collect.foreach(println(_))

    sc.stop()
  }
}

