package operators.repartitioner

import geometry.{Point, Rectangle, Shape}
import operators.selection.partitioner.KeyPartitioner
import org.apache.spark.rdd.RDD

import scala.Array.concat
import scala.math.{max, min}
import scala.reflect.ClassTag

class QuadTreeRepartitioner[T <: Shape : ClassTag](numPartitions: Int,
                                                   var samplingRate: Option[Double] = None,
                                                   threshold: Double = 0)
  extends Repartitioner[T] {

  var partitionRange: Map[Int, Rectangle] = Map()

  override def partition(dataRDD: RDD[T]): RDD[T] = {
    partitionRange = getPartitionRange(dataRDD)
    //    partitionRange.keys.foreach(x => println(partitionRange(x)))
    val partitioner = new KeyPartitioner(exactNumPartitions)
    val boundary = genBoundary(partitionRange)
    val pRDD = assignPartition(dataRDD, partitionRange, boundary)
      .partitionBy(partitioner)
    pRDD.map(_._2)
  }

  var exactNumPartitions: Int = ((numPartitions - 1) / 3) * 3 + 1

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
        Array(lonMin, latMin, lonMax, latMax)))
      r.entries = data
      r.id = 0
      r
    }

    root.capacity = data.length

    var nodeList: scala.collection.mutable.LinkedHashMap[Int, Node[T]] = scala.collection.mutable.LinkedHashMap(0 -> root)

    def partition: scala.collection.mutable.LinkedHashMap[Int, Node[T]] = {
      var currentLeaves = 1
      var idx = 0
      while (currentLeaves < numLeaves) {
        val nodeToSplit = nodeList.values.toArray
          .filter(x => x.isLeaf).maxBy(_.capacity)
        val nodeToSplitId = nodeToSplit.id
        val minLon = nodeToSplit.r.xMin
        val minLat = nodeToSplit.r.yMin
        val maxLon = nodeToSplit.r.xMax
        val maxLat = nodeToSplit.r.yMax
        val cLon = nodeToSplit.r.center().lon
        val cLat = nodeToSplit.r.center().lat
        val SW = new Node[T](Rectangle(Array(minLon, minLat, cLon, cLat)))
        val SE = new Node[T](Rectangle(Array(cLon, minLat, maxLon, cLat)))
        val NE = new Node[T](Rectangle(Array(cLon, cLat, maxLon, maxLat)))
        val NW = new Node[T](Rectangle(Array(minLon, cLat, cLon, maxLat)))
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
      //      val totalArea = nodeList.map(_._2.r.area).sum
      //      println(totalArea)
      //      println(root.r.area)
      //      assert(root.r.area == totalArea)
      //      println(nodeList.values.filter(x => x.isLeaf).size)
      //      nodeList.values.filter(x => x.isLeaf).foreach(x => println(x.r))
      //      println("-----")

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

  def getSamplingRate[T <: Shape : ClassTag](dataRDD: RDD[T]): Double = {
    val dataSize = dataRDD.count
    max(max(min(1000 / dataSize.toDouble, 0.5), 100 * exactNumPartitions / dataSize.toDouble), 0.3)
  }

  /**
   * Generate the whole boundary of the sampled objects
   *
   * @param partitionMap : map of partitionID --> rectangle
   * @return : List(xMin, yMin, xMax, yMax)
   */
  def genBoundary(partitionMap: Map[Int, Rectangle]): List[Double] = {
    val boxes = partitionMap.values.map(_.coordinates)
    val minLon = boxes.map(_.head).min
    val minLat = boxes.map(_ (1)).min
    val maxLon = boxes.map(_ (2)).max
    val maxLat = boxes.map(_.last).max
    List(minLon, minLat, maxLon, maxLat)
  }

  /**
   * Assign partition to each object
   *
   * @param dataRDD      : data RDD
   * @param partitionMap : map of partitionID --> rectangle
   * @param boundary     : the whole boundary of the sampled objects
   * @tparam T :  type pf spatial dataRDD, extending geometry.Shape
   * @return : partitioned RDD of [(partitionNumber, dataRDD)]
   */
  def assignPartition[T <: Shape : ClassTag](dataRDD: RDD[T],
                                             partitionMap: Map[Int, Rectangle],
                                             boundary: List[Double]): RDD[(Int, T)] = {
    dataRDD.take(1).head match {
      case _: Point =>
        val rddWithIndex = dataRDD
          .map(x => {
            val pointShrink = Point(Array(
              min(max(x.asInstanceOf[Point].x, boundary.head), boundary(2)),
              min(max(x.asInstanceOf[Point].y, boundary(1)), boundary(3))
            ))
            (x, partitionMap.filter {
              case (_, v) => pointShrink.inside(v)
            })
          })
          .map(x => (x._1, x._2.keys.head))
          .map(_.swap)
        rddWithIndex
      case _ =>
        val rddWithIndex = dataRDD
          .map(x => {
            val mbr = x.mbr
            val mbrShrink = Rectangle(Array(
              min(max(mbr.xMin, boundary.head), boundary(2)),
              min(max(mbr.yMin, boundary(1)), boundary(3)),
              max(min(mbr.xMax, boundary(2)), boundary.head),
              max(min(mbr.yMax, boundary(3)), boundary(1))))
            (x, partitionMap.filter {
              case (_, v) => v.intersect(mbrShrink)
            })
          })
        rddWithIndex.map(x => (x._1, x._2.keys))
          .flatMapValues(x => x)
          .map(_.swap)
    }
  }

  def getPartitionRange[T <: Shape : ClassTag](dataRDD: RDD[T]): Map[Int, Rectangle] = {
    val sr = samplingRate.getOrElse(getSamplingRate(dataRDD))
    val sampledRDD = dataRDD.sample(withReplacement = false, sr)
    val quadTree = new QuadTree[T](sampledRDD.collect, exactNumPartitions)
    val nodeList = quadTree.partition
      .filter { case (_, v) => v.isLeaf }
      .map(_._2.r)
    //    println("--")
    //    nodeList.foreach(x => println(x))
    //    println("--")
    val boxesWithID = nodeList.zipWithIndex
      .map(x => (x._2, x._1)).toMap // make all leaves have ids 0 to numPartition-1
    if (threshold != 0) boxesWithID.mapValues(rectangle => rectangle.dilate(threshold)).map(identity)
    else boxesWithID
  }

  /**
   * Partition spatial dataRDD and queries simultaneously
   *
   * @param dataRDD  : data RDD
   * @param queryRDD : query RDD
   * @tparam T : type of spatial dataRDD, extending geometry.Shape
   * @return tuple of (RDD[(partitionNumber, dataRDD)], RDD[(partitionNumber, queryRectangle)])
   */
  def copartition[T <: geometry.Shape : ClassTag, R <: geometry.Shape : ClassTag]
  (dataRDD: RDD[T], queryRDD: RDD[R]):
  (RDD[(Int, T)], RDD[(Int, R)]) = {
    val partitionMap = getPartitionRange(dataRDD)
    val partitioner = new KeyPartitioner(numPartitions)
    val boundary = genBoundary(partitionMap)
    val pRDD = assignPartition(dataRDD, partitionMap, boundary)
      .partitionBy(partitioner)
    val pQueryRDD = assignPartition(queryRDD, partitionMap, boundary)
      .partitionBy(partitioner)
    (pRDD, pQueryRDD)
  }
}
