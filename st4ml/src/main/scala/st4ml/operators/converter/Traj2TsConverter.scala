package st4ml.operators.converter

import st4ml.instances._

import scala.reflect.ClassTag
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

class Traj2TimeSeriesConverter(tArray: Array[Duration],
                               override val optimization: String = "rtree") extends Converter {

  val tMap: Array[(Int, Duration)] = tArray.sortBy(_.start).zipWithIndex.map(_.swap)
  var rTree: Option[RTree[Polygon, String]] = None

  def buildRTree(temporals: Array[Duration]): RTree[Polygon, String] = {
    val r = math.min(math.sqrt(temporals.length).toInt, 8)
    var entries = new Array[(Polygon, String, Int)](0)
    for (i <- temporals.zipWithIndex) {
      val p = Extent(0, 0, 1, 1).toPolygon
      p.setUserData(Array(i._1.start.toDouble, i._1.end.toDouble))
      entries = entries :+ (p.copy.asInstanceOf[Polygon], i._2.toString, i._2)
    }
    RTree[Polygon, String](entries, r, dimension = 3)
  }

  def convert[V: ClassTag, D: ClassTag](input: RDD[Trajectory[V, D]]): RDD[TimeSeries[Array[Trajectory[V, D]], None.type]] = {
    type I = Trajectory[V, D]
    type O = TimeSeries[Array[Trajectory[V, D]], None.type]
    if (optimization == "none") {
      input.mapPartitions(partition => {
        val trajs = partition.toArray
        val emptyTs = TimeSeries.empty[I](tArray)
        Iterator(emptyTs.attachInstance(trajs))
      })
    }
    else if (optimization == "rtree") {
      rTree = Some(buildRTree(tMap.map(_._2)))
      val spark = SparkSession.builder().getOrCreate()
      val rTreeBc = spark.sparkContext.broadcast(rTree)
      input.mapPartitions(partition => {
        val trajs = partition.toArray
        val emptyTs = TimeSeries.empty[I](tArray)
        emptyTs.rTree = rTreeBc.value
        Iterator(emptyTs.attachInstanceRTree(trajs))
      })
    }
    else if (optimization == "regular") {
      val tsMin = tMap.head._2.start
      val tsLength = tMap.head._2.seconds
      // val tsMax = tMap.last._2.end
      val tsSlots = tMap.length
      //assert(emptySm.isRegular, "The structure is not regular.")
      input.flatMap(e => {
        val tMin = e.duration.start
        val tMax = e.duration.end
        val idRanges = Range(math.max(0, ((tMin - tsMin) / tsLength).toInt), math.min(tsSlots - 1, ((tMax - tsMin) / tsLength).toInt) + 1)
        idRanges.map(x => (e, x))
      })
        .mapPartitions(partition => {
          val events = partition.toArray.groupBy(_._2).mapValues(x => x.map(_._1))
          val emptySm = TimeSeries.empty[I](tArray)
          Iterator(emptySm.createTimeSeries(events, getPolygonFromInstanceArray))
        })
    }
    else throw new NoSuchElementException
  }

  def convert[V: ClassTag, D: ClassTag,
    V2: ClassTag, D2: ClassTag](input: RDD[Trajectory[V, D]], agg: Array[Trajectory[V, D]] => V2): RDD[TimeSeries[V2, None.type]] = {
    type I = Trajectory[V, D]
    type O = TimeSeries[Array[Trajectory[V, D]], None.type]
    if (optimization == "none") {
      input.mapPartitions(partition => {
        val trajs = partition.toArray
        val emptyTs = TimeSeries.empty[I](tArray)
        Iterator(emptyTs.attachInstance(trajs).mapValue(agg))
      })
    }
    else if (optimization == "rtree") {
      rTree = Some(buildRTree(tMap.map(_._2)))
      val spark = SparkSession.builder().getOrCreate()
      val rTreeBc = spark.sparkContext.broadcast(rTree)
      input.mapPartitions(partition => {
        val trajs = partition.toArray
        val emptyTs = TimeSeries.empty[I](tArray)
        emptyTs.rTree = rTreeBc.value
        Iterator(emptyTs.attachInstanceRTree(trajs).mapValue(agg))
      })
    }
    else if (optimization == "regular") {
      val emptyTs = TimeSeries.empty[I](tArray)
      val tsMin = tMap.head._2.start
      val tsLength = tMap.head._2.seconds
      // val tsMax = tMap.last._2.end
      val tsSlots = tMap.length
      //assert(emptySm.isRegular, "The structure is not regular.")
      input.flatMap(e => {
        val tMin = e.duration.start
        val tMax = e.duration.end
        val idRanges = Range(math.max(0, ((tMin - tsMin) / tsLength).toInt), math.min(tsSlots - 1, ((tMax - tsMin) / tsLength).toInt) + 1)
        idRanges.map(x => (e, x))
      })
        .mapPartitions(partition => {
          val events = partition.toArray.groupBy(_._2).mapValues(x => x.map(_._1))
          val emptySm = TimeSeries.empty[I](tArray)
          val f: Array[Trajectory[V, D]] => Polygon = _ => Polygon.empty
          Iterator(emptySm.createTimeSeries(events, f).mapValue(agg))
        })
    }
    else throw new NoSuchElementException
  }

  def convert[V: ClassTag, D: ClassTag,
    V2: ClassTag, D2: ClassTag,
    V3: ClassTag](input: RDD[Trajectory[V, D]], preMap: Trajectory[V, D] => Trajectory[V2, D2],
                  agg: Array[Trajectory[V2, D2]] => V3): RDD[TimeSeries[V3, None.type]] = {
    type I = Trajectory[V2, D2]
    type O = TimeSeries[V3, None.type]
    if (optimization == "none") {
      input.map(preMap).mapPartitions(partition => {
        val trajs = partition.toArray
        val emptyTs = TimeSeries.empty[I](tArray)
        Iterator(emptyTs.attachInstance(trajs).mapValue(agg))
      })
    }
    else if (optimization == "rtree") {
      rTree = Some(buildRTree(tMap.map(_._2)))
      val spark = SparkSession.builder().getOrCreate()
      val rTreeBc = spark.sparkContext.broadcast(rTree)
      input.map(preMap).mapPartitions(partition => {
        val trajs = partition.toArray
        val emptyTs = TimeSeries.empty[I](tArray)
        emptyTs.rTree = rTreeBc.value
        Iterator(emptyTs.attachInstanceRTree(trajs).mapValue(agg))
      })
    }
    else if (optimization == "regular") {
      val emptyTs = TimeSeries.empty[I](tArray)
      val tsMin = tMap.head._2.start
      val tsLength = tMap.head._2.seconds
      // val tsMax = tMap.last._2.end
      val tsSlots = tMap.length
      //assert(emptySm.isRegular, "The structure is not regular.")
      input.map(preMap).flatMap(e => {
        val tMin = e.duration.start
        val tMax = e.duration.end
        val idRanges = Range(math.max(0, ((tMin - tsMin) / tsLength).toInt), math.min(tsSlots - 1, ((tMax - tsMin) / tsLength).toInt) + 1)
        idRanges.map(x => (e, x))
      })
        .mapPartitions(partition => {
          val events = partition.toArray.groupBy(_._2).mapValues(x => x.map(_._1))
          val emptySm = TimeSeries.empty[I](tArray)
          val f: Array[Trajectory[V2, D2]] => Polygon = _ => Polygon.empty
          Iterator(emptySm.createTimeSeries(events, f).mapValue(agg))
        })
    }
    else throw new NoSuchElementException
  }

  def getPolygonFromInstanceArray[I <: Trajectory[_, _]](instanceArr: Array[I]): Polygon = {
    if (instanceArr.nonEmpty) {
      Extent(instanceArr.map(_.extent)).toPolygon
    }
    else Polygon.empty
  }

}

object Traj2TimeSeriesConverterTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").getOrCreate()
    val sc = spark.sparkContext

    val trajs = Array(
      Trajectory(Array(Point(1, 2), Point(3, 4)), Array(Duration(34), Duration(94)), Array(None, None), "0"),
      Trajectory(Array(Point(5, 6), Point(7, 8)), Array(Duration(134), Duration(274)), Array(None, None), "1"),
      Trajectory(Array(Point(9, 10), Point(11, 12)), Array(Duration(234), Duration(284)), Array(None, None), "2"),
      Trajectory(Array(Point(13, 14), Point(15, 16)), Array(Duration(334), Duration(364)), Array(None, None), "3")
    )

    val eventRDD = sc.parallelize(trajs)

    val tArray = Array(
      Duration(0, 100), // 1
      Duration(100, 200), // 1
      Duration(200, 300), // 2
      Duration(300, 400) // 1
    )

    val f: Array[Trajectory[None.type, String]] => Int = _.length

    //    val f: Array[Trajectory[None.type, String]] => Array[Trajectory[None.type, String]] = x => x
    val countConverter = new Traj2TimeSeriesConverter(tArray)

    val tsRDD = countConverter.convert(eventRDD)
    tsRDD.collect.foreach(println(_))

    sc.stop()
  }
}