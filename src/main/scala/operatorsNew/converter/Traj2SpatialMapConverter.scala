package operatorsNew.converter

import instances.{Duration, Entry, Extent, Point, Polygon, RTree, SpatialMap, Trajectory}
import operators.selection.indexer.RTreeDeprecated
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

class Traj2SpatialMapConverter[V, D, VSM, DSM](f: Array[Trajectory[V, D]] => VSM,
                                               sArray: Array[Polygon],
                                               d: DSM = None) extends Converter {
  type I = Trajectory[V, D]
  type O = SpatialMap[VSM, DSM]
  val sMap: Array[(Int, Polygon)] = sArray.sortBy(x =>
    (x.getCoordinates.map(c => c.x).min, x.getCoordinates.map(c => c.y).min)).zipWithIndex.map(_.swap)
  var rTreeDeprecated: Option[RTreeDeprecated[geometry.Rectangle]] = None
  var rTree: Option[RTree[Polygon]] = None

  lazy val smXMin: Double = sMap.head._2.getEnvelopeInternal.getMinX
  lazy val smYMin: Double = sMap.head._2.getEnvelopeInternal.getMinY
  lazy val smXMax: Double = sMap.last._2.getEnvelopeInternal.getMaxX
  lazy val smYMax: Double = sMap.last._2.getEnvelopeInternal.getMaxY
  lazy val smXLength: Double = sMap.head._2.getEnvelopeInternal.getMaxX - smXMin
  lazy val smYLength: Double = sMap.head._2.getEnvelopeInternal.getMaxY - smYMin
  lazy val smXSlots: Long = ((smXMax - smXMin) / smXLength).round
  lazy val smYSlots: Long = ((smYMax - smYMin) / smYLength).round

  //  def buildRTreeDeprecated(spatials: Array[Polygon]): RTreeDeprecated[geometry.Rectangle] = {
  //    val r = math.sqrt(spatials.length).toInt
  //    val entries = spatials.map(s => {
  //      val e = Extent(s.getEnvelopeInternal)
  //      geometry.Rectangle(Array(e.xMin, e.yMin, e.xMax, e.yMax))
  //    }).zipWithIndex.map(x => (x._1, x._2.toString, x._2))
  //    RTreeDeprecated[geometry.Rectangle](entries, r)
  //  }
  def buildRTree(spatials: Array[Polygon]): RTree[Polygon] = {
    val r = math.sqrt(spatials.length).toInt
    val entries = spatials.zipWithIndex.map(x => (x._1, x._2.toString, x._2))
    RTree[Polygon](entries, r)
  }

  override def convert(input: RDD[I]): RDD[O] = {
    input.mapPartitions(partition => {
      val trajs = partition.toArray
      val emptySm = SpatialMap.empty[I](sArray)
      Iterator(emptySm.attachInstance(trajs)
        .mapValue(f)
        .mapData(_ => d))
    })
  }

  /** use MBR instead of line string for overlapping */
  def convertFast(input: RDD[I]): RDD[O] = {
    input.mapPartitions(partition => {
      val trajs = partition.toArray
      val emptySm = SpatialMap.empty[I](sArray)
      Iterator(emptySm.attachInstance(trajs, trajs.map(_.extent.toPolygon))
        .mapValue(f)
        .mapData(_ => d))
    })
  }

  def convertRegular(input: RDD[I]): RDD[O] = {
    val emptySm = SpatialMap.empty[I](sArray)
    //assert(emptySm.isRegular, "The structure is not regular.")
    input.flatMap(e => {
      val xMin = e.extent.xMin
      val xMax = e.extent.xMax
      val yMin = e.extent.yMin
      val yMax = e.extent.yMax
      val xRanges = (math.max(0, ((xMin - smXMin) / smXLength).toInt), math.min(smXSlots - 1, ((xMax - smXMin) / smXLength).toInt))
      val yRanges = (math.max(0, ((yMin - smYMin) / smYLength).toInt), math.min(smXSlots - 1, ((yMax - smYMin) / smYLength).toInt))
      val idRanges = Range((xRanges._1 * smYSlots + yRanges._1).toInt, (xRanges._2 * smYSlots + yRanges._2).toInt + 1, 1).toArray
      idRanges.map(x => (e, x))
    })
      .mapPartitions(partition => {
        val events = partition.toArray.groupBy(_._2).mapValues(x => x.map(_._1)) .map {
                  case (id, instanceArr) =>
                    (id, instanceArr.filter(x => x.toGeometry.intersects(sMap(id)._2)))
                }
        val emptySm = SpatialMap.empty[I](sArray)
        Iterator(emptySm.createSpatialMap(events)
          .mapValue(f)
          .mapData(_ => d))
      })
  }


  //  // using MBR, not precise
  //  def convertWithRTreeDeprecated(input: RDD[I]): RDD[O] = {
  //    rTreeDeprecated = Some(buildRTreeDeprecated(sMap.map(_._2)))
  //    val spark = SparkSession.builder().getOrCreate()
  //    val rTreeBc = spark.sparkContext.broadcast(rTreeDeprecated)
  //    input.mapPartitions(partition => {
  //      val trajs = partition.toArray
  //      val emptySm = SpatialMap.empty[I](sArray)
  //      emptySm.rTreeDeprecated = rTreeBc.value
  //      Iterator(emptySm.attachInstanceRTreeDeprecated(trajs, trajs.map(_.extent.toPolygon))
  //        .mapValue(f)
  //        .mapData(_ => d))
  //    })
  //  }
  // using MBR, not precise
  def convertWithRTree(input: RDD[I]): RDD[O] = {
    rTree = Some(buildRTree(sMap.map(_._2)))
    val spark = SparkSession.builder().getOrCreate()
    val rTreeBc = spark.sparkContext.broadcast(rTree)
    input.mapPartitions(partition => {
      val trajs = partition.toArray
      val emptySm = SpatialMap.empty[I](sArray)
      emptySm.rTree = rTreeBc.value
      Iterator(emptySm.attachInstanceRTree(trajs, trajs.map(_.toGeometry))
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