package operatorsNew.converter

import instances.{Duration, Entry, Geometry, LineString, Polygon, RTree, Raster, Trajectory}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag


class Traj2RasterConverter(polygonArr: Array[Polygon],
                           durArr: Array[Duration],
                           override val optimization: String = "rtree") extends Converter {
  var rTree: Option[RTree[Polygon]] = None

  def buildRTree(polygonArr: Array[Polygon],
                 durArr: Array[Duration]): RTree[Polygon] = {
    val r = (math.pow(polygonArr.length, 1.0 / 3) / 2).toInt
    var entries = new Array[(Polygon, String, Int)](0)
    for (i <- polygonArr.indices) {
      polygonArr(i).setUserData(Array(durArr(i).start.toDouble, durArr(i).end.toDouble))
      entries = entries :+ (polygonArr(i).copy.asInstanceOf[Polygon], i.toString, i)
    }
    RTree[Polygon](entries, r, dimension = 3)
  }

  def convert[V: ClassTag, D: ClassTag](input: RDD[Trajectory[V, D]]): RDD[Raster[Polygon, Array[Trajectory[V, D]], None.type]] = {
    type I = Trajectory[V, D]
    type O = Raster[Polygon, Array[I], None.type]
    if (optimization == "none") {
      input.mapPartitions(partition => {
        val trajs = partition.toArray
        val emptyRaster = Raster.empty[I](polygonArr, durArr)
        Iterator(emptyRaster.attachInstance(trajs))
      })
    }
    else if (optimization == "rtree") {
      rTree = Some(buildRTree(polygonArr, durArr))
      val spark = SparkSession.builder().getOrCreate()
      val rTreeBc = spark.sparkContext.broadcast(rTree)
      input.mapPartitions(partition => {
        val trajs = partition.toArray
        val emptyRaster = Raster.empty[I](polygonArr, durArr)
        emptyRaster.rTree = rTreeBc.value
        Iterator(emptyRaster.attachInstanceRTree(trajs))
      })
    }
    else throw new NoSuchElementException
  }

  def convert[S <: Geometry : ClassTag, V: ClassTag, D: ClassTag,
    V2: ClassTag, D2: ClassTag](input: RDD[Trajectory[V, D]], agg: Array[Trajectory[V, D]] => V2): RDD[Raster[Polygon, V2, None.type]] = {
    type I = Trajectory[V, D]
    type O = Raster[Polygon, Array[I], None.type]
    if (optimization == "none") {
      input.mapPartitions(partition => {
        val trajs = partition.toArray
        val emptyRaster = Raster.empty[I](polygonArr, durArr)
        Iterator(emptyRaster.attachInstance(trajs).mapValue(agg))
      })
    }
    else if (optimization == "rtree") {
      rTree = Some(buildRTree(polygonArr, durArr))
      val spark = SparkSession.builder().getOrCreate()
      val rTreeBc = spark.sparkContext.broadcast(rTree)
      input.mapPartitions(partition => {
        val trajs = partition.toArray
        val emptyRaster = Raster.empty[I](polygonArr, durArr)
        emptyRaster.rTree = rTreeBc.value
        Iterator(emptyRaster.attachInstanceRTree(trajs).mapValue(agg))
      })
    }
    else throw new NoSuchElementException
  }

  def convert[V: ClassTag, D: ClassTag,
    V2: ClassTag, D2: ClassTag,
    V3: ClassTag](input: RDD[Trajectory[V, D]],
                  preMap: Trajectory[V, D] => Trajectory[V2, D2],
                  agg: Array[Trajectory[V2, D2]] => V3): RDD[Raster[Polygon, V3, None.type]] = {
    type I = Trajectory[V2, D2]
    type O = Raster[Polygon, Array[I], None.type]
    if (optimization == "none") {
      input.map(preMap).mapPartitions(partition => {
        val trajs = partition.toArray
        val emptyRaster = Raster.empty[I](polygonArr, durArr)
        Iterator(emptyRaster.attachInstance(trajs).mapValue(agg))
      })
    }
    else if (optimization == "rtree") {
      rTree = Some(buildRTree(polygonArr, durArr))
      val spark = SparkSession.builder().getOrCreate()
      val rTreeBc = spark.sparkContext.broadcast(rTree)
      input.map(preMap).mapPartitions(partition => {
        val trajs = partition.toArray
        val emptyRaster = Raster.empty[I](polygonArr, durArr)
        emptyRaster.rTree = rTreeBc.value
        Iterator(emptyRaster.attachInstanceRTree(trajs).mapValue(agg))
      })
    }
    else throw new NoSuchElementException
  }
}