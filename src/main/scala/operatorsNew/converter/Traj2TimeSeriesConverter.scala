package operatorsNew.converter

import instances.{Duration, Entry, Extent, Point, Polygon, RTree, TimeSeries, Trajectory}
//import intervalTree.mutable.IntervalTree
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
//import intervalTree.Interval

class Traj2TimeSeriesConverter[V, D, VTS, DTS](f: Array[Trajectory[V, D]] => VTS,
                                               tArray: Array[Duration],
                                               d: DTS = None) extends Converter {
  type I = Trajectory[V, D]
  type O = TimeSeries[VTS, DTS]
  val tMap: Array[(Int, Duration)] = tArray.zipWithIndex.map(_.swap)
  var rTree: Option[RTree[Polygon]] = None
//  var intervalTree: Option[IntervalTree[Int]] = None
//
//  def buildIntervalTree(temporals: Array[Duration]): IntervalTree[Int] = {
//    val intervals = temporals.zipWithIndex.map(x => new Interval[Int](x._1.start, x._1.end, x._2))
//    IntervalTree(intervals)
//  }

  def buildRTree(temporals: Array[Duration]): RTree[Polygon] = {
    val r = math.sqrt(temporals.length).toInt
    var entries = new Array[(Polygon, String, Int)](0)
    for (i <- temporals.zipWithIndex) {
      val p = Extent(0, 0, 1, 1).toPolygon
      p.setUserData(Array(i._1.start.toDouble, i._1.end.toDouble))
      entries = entries :+ (p.copy.asInstanceOf[Polygon], i._2.toString, i._2)
    }
    RTree[Polygon](entries, r, dimension = 3)
  }

  override def convert(input: RDD[I]): RDD[O] = {
    input.mapPartitions(partition => {
      val trajs = partition.toArray
      val emptyTs = TimeSeries.empty[I](tArray)
      Iterator(emptyTs.attachInstance(trajs)
        .mapValue(f)
        .mapData(_ => d))
    })
  }

  def convertWithRTree(input: RDD[I]): RDD[O] = {
    rTree = Some(buildRTree(tMap.map(_._2)))
    val spark = SparkSession.builder().getOrCreate()
    val rTreeBc = spark.sparkContext.broadcast(rTree)
    input.mapPartitions(partition => {
      val trajs = partition.toArray
      val emptyTs = TimeSeries.empty[I](tArray)
      emptyTs.rTree = rTreeBc.value
      Iterator(emptyTs.attachInstanceRTree(trajs)
        .mapValue(f)
        .mapData(_ => d))
    })
  }
//  def convertWithIntervalTree(input: RDD[I]): RDD[O] = {
////    val spark = SparkSession.builder().getOrCreate()
////    val rTreeBc = spark.sparkContext.broadcast(intervalTree)
//    input.mapPartitions(partition => {
//      val intervalTree = Some(buildIntervalTree(tMap.map(_._2)))
//      val trajs = partition.toArray
//      val emptyTs = TimeSeries.empty[I](tArray)
//      emptyTs.intervalTree = intervalTree
//      Iterator(emptyTs.attachInstanceIntervalTree(trajs)
//        .mapValue(f)
//        .mapData(_ => d))
//    })
//  }
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
    val countConverter = new Traj2TimeSeriesConverter(f, tArray)

    val tsRDD = countConverter.convert(eventRDD)
    tsRDD.collect.foreach(println(_))

    sc.stop()
  }
}