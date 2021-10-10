package operatorsNew.converter

import instances.{Duration, Entry, Extent, Point, Polygon, SpatialMap, Trajectory}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

class Traj2SpatialMapConverter[V, D, VSM, DSM](f: Array[Trajectory[V, D]] => VSM,
                                                              sArray: Array[Polygon],
                                                              d: DSM = None) extends Converter {
  type I = Trajectory[V, D]
  type O = SpatialMap[VSM, DSM]
  val sMap: Array[(Int, Polygon)] = sArray.zipWithIndex.map(_.swap)
  override def convert(input: RDD[I]): RDD[O] = {
    input.mapPartitions(partition => {
      val trajs = partition.toArray
      val emptySm = SpatialMap.empty[I](sArray)
      Iterator(emptySm.attachInstance(trajs)
        .mapValue(f)
        .mapData(_ => d))
    })
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
    val countConverter = new Traj2SpatialMapConverter(f, sArray)

    val tsRDD = countConverter.convert(eventRDD)
    tsRDD.collect.foreach(println(_))

    sc.stop()
  }
}