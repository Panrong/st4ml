package operatorsNew.converter

import instances.{Duration, Entry, Geometry, LineString, Polygon, RTree, Raster, Trajectory}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

class Traj2RasterConverter[V, D, VR, DR](f: Array[Trajectory[V, D]] => VR,
                                         polygonArr: Array[Polygon],
                                         durArr: Array[Duration],
                                         d: DR = None) extends Converter {
  type I = Trajectory[V, D]
  type O = Raster[Polygon, VR, DR]
  var rTree: Option[RTree[Polygon]] = None

  def buildRTree(polygonArr: Array[Polygon],
                 durArr: Array[Duration]): RTree[Polygon] = {
    val r = math.pow(polygonArr.length, 1.0 / 3).toInt
    var entries = new Array[(Polygon, String, Int)](0)
    for (i <- polygonArr.indices) {
      polygonArr(i).setUserData(Array(durArr(i).start.toDouble, durArr(i).end.toDouble))
      entries = entries :+ (polygonArr(i).copy.asInstanceOf[Polygon], i.toString, i)
    }
    RTree[Polygon](entries, r, dimension = 3)
  }

  override def convert(input: RDD[I]): RDD[O] = {
    input.mapPartitions(partition => {
      val trajs = partition.toArray
      val emptyRaster = Raster.empty[I](polygonArr, durArr)
      Iterator(emptyRaster.attachInstance(trajs) // use geometry intersection
        .mapValue(f)
        .mapData(_ => d))
    })
  }

  def convertWithRTree(input: RDD[I]): RDD[O] = {
    rTree = Some(buildRTree(polygonArr, durArr))
    val spark = SparkSession.builder().getOrCreate()
    val rTreeBc = spark.sparkContext.broadcast(rTree)
    input.mapPartitions(partition => {
      val trajs = partition.toArray
      val emptyRaster = Raster.empty[I](polygonArr, durArr)
      emptyRaster.rTree = rTreeBc.value
      Iterator(emptyRaster.attachInstanceRTree(trajs)
        .mapValue(f)
        .mapData(_ => d))
    })
  }
}