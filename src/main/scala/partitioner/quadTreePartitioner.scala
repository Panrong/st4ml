package partitioner

import org.apache.spark.Partitioner
import org.apache.spark.rdd.{RDD, ShuffledRDD}
import geometry.{Point, Rectangle, Shape}

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.math.{max, min}
import Array.concat

object quadTreePartitioner {
  /**
   * QuadTree partitioner
   *
   * @param r            : input RDD
   * @param numPartition : number of partitions, has to be to 3k+1 (otherwise is rounded)
   * @param samplingRate : sample some data to determine the boundary, less than 1
   * @tparam T : type extends Shape
   * @return partitioned RDD, quadTree and a map of QuadTreeID -> PartitionNum (to record the boundary of each partition)
   */
  def apply[T <: Shape : ClassTag](r: RDD[T], numPartition: Int,
                                   samplingRate: Double): (RDD[T], QuadTree[T], Map[Int, Int]) = {
    val n = ((numPartition - 1) / 3) * 3 + 1
    val sampledRDD = r.sample(withReplacement = false, samplingRate)
    val quadTree = new QuadTree[T](sampledRDD.collect, n)
    val nodeList = quadTree.partition.filter { case (_, v) => v.isLeaf }
      .map { case (k, v) => (v.r, k) }
    val nodeIdPartitionMap = nodeList.zipWithIndex
      .map(x => (x._1._2, x._2)).toMap
      .withDefaultValue(-1) // nodeID -> partitionID
    val reIdNodeList = nodeList.zipWithIndex
      .map(x => (x._1._1, x._2)) // make all leaves have ids 0 to numPartition-1
    val partitioner = new quadTreePartitioner[T](n, reIdNodeList)
    val pRDD = new ShuffledRDD[T, T, T](r.map(x => (x, x)), partitioner)
    (pRDD.map(x => x._1), quadTree, nodeIdPartitionMap)
  }
}

class quadTreePartitioner[T <: Shape : ClassTag](num: Int, p: mutable.LinkedHashMap[Rectangle, Int]) extends Partitioner with Serializable {
  override def numPartitions: Int = num

  val minLon: Double = p.keys.map(_.x_min).toArray.min
  val minLat: Double = p.keys.map(_.y_min).toArray.min
  val maxLon: Double = p.keys.map(_.x_max).toArray.max
  val maxLat: Double = p.keys.map(_.y_max).toArray.max

  override def getPartition(key: Any): Int = {
    val c = key.asInstanceOf[Shape].center()
    val k = Point(max(min(c.lon, maxLon), minLon), max(min(c.lat, maxLat), minLat))
    val res = p.filterKeys(k.inside).values.toArray
    res(0)
  }
}

class Node[T <: Shape : ClassTag](range: Rectangle) extends Serializable {
  val r: Rectangle = range
  var childNW: Int = 0
  var childNE: Int = 0
  var childSW: Int = 0
  var childSE: Int = 0
  var isLeaf: Boolean = true
  var id: Int = -1
  var capacity: Int = 0
  var entries = new Array[T](0)
}

class QuadTree[T <: Shape : ClassTag](data: Array[T], numLeaves: Int) extends Serializable {

  val root: Node[T] = {
    val lons = data.map(x => x.center().lon).sorted
    val lonMin = lons(0)
    val lonMax = lons.last
    val lats = data.map(x => x.center().lat).sorted
    val latMin = lats(0)
    val latMax = lats.last
    val r = new Node[T](Rectangle(
      Point(lonMin, latMin), Point(lonMax, latMax)))
    r.entries = data
    r.id = 0
    r
  }

  root.capacity = data.length

  var nodeList = scala.collection.mutable.LinkedHashMap(0 -> root)

  def partition: scala.collection.mutable.LinkedHashMap[Int, Node[T]] = {
    var currentLeaves = 1
    var idx = 0
    while (currentLeaves < numLeaves) {
      val nodeToSplit = nodeList.values.toArray
        .filter(x => x.isLeaf).maxBy(_.capacity)
      val nodeToSplitId = nodeToSplit.id
      val minLon = nodeToSplit.r.bottomLeft.lon
      val minLat = nodeToSplit.r.bottomLeft.lat
      val maxLon = nodeToSplit.r.topRight.lon
      val maxLat = nodeToSplit.r.topRight.lat
      val cLon = nodeToSplit.r.center().lon
      val cLat = nodeToSplit.r.center().lat
      val SW = new Node[T](Rectangle(nodeToSplit.r.bottomLeft, nodeToSplit.r.center()))
      val SE = new Node[T](Rectangle(Point(cLon, minLat), Point(maxLon, cLat)))
      val NE = new Node[T](Rectangle(nodeToSplit.r.center(), nodeToSplit.r.topRight))
      val NW = new Node[T](Rectangle(Point(minLon, cLat), Point(cLon, maxLat)))
      SW.id = idx + 1
      SE.id = idx + 2
      NW.id = idx + 3
      NE.id = idx + 4
      idx = idx + 4
      nodeList(nodeToSplitId).childNW = NW.id
      nodeList(nodeToSplitId).childNE = NE.id
      nodeList(nodeToSplitId).childSW = SW.id
      nodeList(nodeToSplitId).childSE = SE.id
      nodeList(nodeToSplitId).isLeaf = false
      currentLeaves += 3
      nodeToSplit.entries.foreach { x => {
        if (x.inside(NW.r)) NW.entries = NW.entries :+ x
        else if (x.inside(SW.r)) SW.entries = SW.entries :+ x
        else if (x.inside(NE.r)) NE.entries = NE.entries :+ x
        else SE.entries = SE.entries :+ x
      }
      }
      for (node <- List(SW, SE, NW, NE)) {
        nodeList.put(node.id, node)
        node.capacity = node.entries.length
      }
      nodeToSplit.entries = new Array[T](0)
    }
    nodeList
  }

  def query(rectangle: Rectangle, node: Node[T] = root, r: Array[Int] = new Array[Int](0)): Array[Int] = {
    var res = r
    queryNode(rectangle, node) match {
      case Left(children) => children.foreach(child => res = concat(res, query(rectangle, nodeList(child))))
      case Right(node) => res = res :+ node
    }
    res
  }

  def queryNode(rectangle: Rectangle, node: Node[T]): Either[Array[Int], Int] = {
    if (node.isLeaf) Right(node.id)
    else {
      var res = new Array[Int](0)
      if (node.r.center().inside(rectangle))
        Left(Array(node.childNW, node.childNE, node.childSE, node.childSW))
      else {
        for (i <- Array(node.childNW, node.childNE, node.childSE, node.childSW)) {
          if (nodeList(i).r.intersect(rectangle)) res = res :+ i
        }
        Left(res)
      }
    }
  }
}

