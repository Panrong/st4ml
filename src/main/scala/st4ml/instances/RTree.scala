package st4ml.instances

import scala.collection.mutable
import scala.util.control.Breaks
import scala.reflect.ClassTag
import st4ml.instances.GeometryImplicits._

abstract class RTreeEntry {
  def minDist(x: Geometry): Double

  def intersect(x: Geometry): Boolean
}

case class RTreeLeafEntry[T <: Geometry : ClassTag, D: ClassTag](shape: T, mData: D, size: Int) extends RTreeEntry {
  override def minDist(x: Geometry): Double = shape.distance(x)

  override def intersect(x: Geometry): Boolean = x.intersects(shape)
}

case class RTreeInternalEntry[D: ClassTag](mbr: Polygon, node: RTreeNode[D]) extends RTreeEntry {
  override def minDist(x: Geometry): Double = mbr.distance(x)

  override def intersect(x: Geometry): Boolean = x.intersects(mbr)
}

case class RTreeNode[D: ClassTag](mMbr: Polygon, mChild: Array[RTreeEntry], isLeaf: Boolean) {
  val size: Long = {
    if (isLeaf) mChild.map(x => x.asInstanceOf[RTreeLeafEntry[Geometry, D]].size).sum
    else mChild.map(x => x.asInstanceOf[RTreeInternalEntry[D]].node.size).sum
  }
}

object RTreeNode {
  def apply[D: ClassTag](mMbr: Polygon, children: Array[(Polygon, RTreeNode[D])]): RTreeNode[D] = {
    RTreeNode(mMbr, children.map(x => RTreeInternalEntry(x._1, x._2)), isLeaf = false)
  }

  def apply[T <: Geometry : ClassTag, D: ClassTag](mMbr: Polygon, children: => Array[(T, D)]): RTreeNode[D] = {
    RTreeNode(mMbr, children.map(x => RTreeLeafEntry(x._1, x._2, 1)), isLeaf = true)
  }

  def apply[T <: Geometry : ClassTag, D: ClassTag](mMbr: Polygon, children: Array[(T, D, Int)]): RTreeNode[D] = {
    RTreeNode(mMbr, children.map(x => RTreeLeafEntry(x._1, x._2, x._3)), isLeaf = true)
  }
}

class NNOrdering() extends Ordering[(_, Double)] {
  def compare(a: (_, Double), b: (_, Double)): Int = -a._2.compare(b._2)
}

case class RTree[T <: Geometry : ClassTag, D: ClassTag](root: RTreeNode[D]) extends Serializable {
  var numEntries: Int = 0
  //  var count: Int = 0

  def setNumEntries(n: Int): RTree[T, D] = {
    numEntries = n
    this
  }

  def range[Q <: Geometry : ClassTag](query: Q): Array[(T, D)] = {
    val ans = mutable.ArrayBuffer[(T, D)]()
    val st = new mutable.Stack[RTreeNode[D]]()
    if (root.mMbr.intersects(query) && root.mChild.nonEmpty) st.push(root)
    while (st.nonEmpty) {
      val now = st.pop()
      if (!now.isLeaf) {
        now.mChild.foreach {
          x =>
            val y = x.asInstanceOf[RTreeInternalEntry[D]]
            if (query.intersects(y.mbr)) st.push(y.node)
        }
      } else {
        now.mChild.foreach {
          x =>
            val y = x.asInstanceOf[RTreeLeafEntry[T, D]]
            if (query.intersects(y.shape)) ans += ((y.shape, y.mData))
        }
      }
    }
    ans.toArray
  }

  def distanceRange[Q <: Geometry : ClassTag](query: Q, distance: Double, meter: Boolean = true): Array[(T, D)] = {
    val d = if (meter) distance / 111000 else distance
    val ans = mutable.ArrayBuffer[(T, D)]()
    val st = new mutable.Stack[RTreeNode[D]]()
    if (root.mMbr.intersects(query) && root.mChild.nonEmpty) st.push(root)
    while (st.nonEmpty) {
      val now = st.pop()
      if (!now.isLeaf) {
        now.mChild.foreach {
          x =>
            val y = x.asInstanceOf[RTreeInternalEntry[D]]
            if (query.intersects(y.mbr)) st.push(y.node)
        }
      } else {
        now.mChild.foreach {
          x =>
            val y = x.asInstanceOf[RTreeLeafEntry[T, D]]
            if (query.intersects(y.shape)) ans += ((y.shape, y.mData))
        }
      }
    }
    ans.toArray
  }

  def intersect[S <: Geometry : ClassTag](shape: S, extent: Extent, duration: Duration): Boolean = {
    //             println(shape)
    //             println(instance)
    //             println(Duration(shape.getUserData.asInstanceOf[Array[Double]].map(_.toLong)))
    //             println( shape.intersects(instance.toGeometry) &&
    //               Duration(shape.getUserData.asInstanceOf[Array[Double]].map(_.toLong)).intersects(instance.duration)
    //             )
    val dur = shape.getUserData.asInstanceOf[Array[Double]]
    shape.intersects(extent) &&
      !(dur(1) < duration.start || duration.end < dur(0))
  }

  def intersect[S <: Geometry : ClassTag](shape: S, geometry: Geometry, duration: Duration): Boolean = {
    //             println(shape)
    //             println(instance)
    //             println(Duration(shape.getUserData.asInstanceOf[Array[Double]].map(_.toLong)))
    //             println( shape.intersects(instance.toGeometry) &&
    //               Duration(shape.getUserData.asInstanceOf[Array[Double]].map(_.toLong)).intersects(instance.duration)
    //             )
    val dur = shape.getUserData.asInstanceOf[Array[Double]]
    shape.intersects(geometry) &&
      !(dur(1) < duration.start || duration.end < dur(0))
  }

  def range3d[Q <: Instance[_, _, _] : ClassTag](query: Q): Array[(T, D)] = {
    val ans = mutable.ArrayBuffer[(T, D)]()
    val extent = query.extent
    val geometry = query.toGeometry
    val duration = query.duration
    val st = new mutable.Stack[RTreeNode[D]]()
    if (intersect(root.mMbr, extent, duration) && root.mChild.nonEmpty) st.push(root)
    while (st.nonEmpty) {
      val now = st.pop()
      if (!now.isLeaf) {
        now.mChild.foreach {
          x =>
            val y = x.asInstanceOf[RTreeInternalEntry[D]]
            if (query.intersects(y.mbr)) st.push(y.node)
        }
      } else {
        now.mChild.foreach {
          x =>
            val y = x.asInstanceOf[RTreeLeafEntry[T, D]]
            if (intersect(y.shape, geometry, duration)) ans += ((y.shape, y.mData))
        }
      }
    }
    ans.toArray
  }

  def intersects1d[I <: Geometry](a: I, b: (Long, Long)): Boolean = {
    val dur = a.getUserData.asInstanceOf[Array[Double]]
    //    count += 1
    !(dur(1) < b._1 || b._2 < dur(0))
  }

  def range1d[Q <: Geometry : ClassTag](query: (Long, Long)): Array[(T, D)] = {
    val ans = mutable.ArrayBuffer[(T, D)]()
    var st = List[RTreeNode[D]]()
    if (intersects1d(root.mMbr, query) && root.mChild.nonEmpty) st = st :+ root
    while (st.nonEmpty) {
      val now = st.last
      st = st.dropRight(1)
      if (!now.isLeaf) {
        now.mChild.foreach {
          x =>
            val y = x.asInstanceOf[RTreeInternalEntry[D]]
            if (intersects1d(y.mbr, query)) st = st :+ y.node
        }
      } else {
        now.mChild.foreach {
          x =>
            val y = x.asInstanceOf[RTreeLeafEntry[T, D]]
            if (intersects1d(y.shape, query)) ans += ((y.shape, y.mData))
        }
      }
    }
    //    println(s"..$count")
    //    this.count = 0
    ans.toArray
  }

  def range(query: Polygon, levelLimit: Int, sThreshold: Double): Option[Array[(T, D)]] = {
    val ans = mutable.ArrayBuffer[(T, D)]()
    val q = new mutable.Queue[(RTreeNode[D], Int)]()
    if (root.mMbr.intersects(query) && root.mChild.nonEmpty) q.enqueue((root, 1))
    var estimate: Double = 0
    val loop = new Breaks
    import loop.{break, breakable}
    breakable {
      while (q.nonEmpty) {
        val now = q.dequeue
        val cur_node = now._1
        val cur_level = now._2
        if (cur_node.isLeaf) {
          cur_node.mChild.foreach {
            x =>
              val y = x.asInstanceOf[RTreeLeafEntry[T, D]]
              if (query.intersects(y.shape)) ans += ((y.shape, y.mData))
          }
        } else if (cur_level < levelLimit) {
          cur_node.mChild.foreach {
            x =>
              val y = x.asInstanceOf[RTreeInternalEntry[D]]
              if (query.intersects(y.mbr)) q.enqueue((y.node, cur_level + 1))
          }
        } else if (cur_level == levelLimit) {

          val overlappingRatio = {
            try cur_node.mMbr.intersection(query).getArea / cur_node.mMbr.getArea
            catch {
              case _: Throwable => 0.0
            }
          }
          estimate += overlappingRatio * cur_node.size
          cur_node.mChild.foreach {
            x =>
              val y = x.asInstanceOf[RTreeInternalEntry[D]]
              if (query.intersects(y.mbr)) q.enqueue((y.node, cur_level + 1))
          }
        } else break
      }
    }
    if (ans.nonEmpty) return Some(ans.toArray)
    else if (estimate / root.size > sThreshold) return None
    while (q.nonEmpty) {
      val now = q.dequeue
      val cur_node = now._1
      val cur_level = now._2
      if (cur_node.isLeaf) {
        cur_node.mChild.foreach {
          x =>
            val y = x.asInstanceOf[RTreeLeafEntry[T, D]]
            if (query.intersects(y.shape)) ans += ((y.shape, y.mData))
        }
      } else {
        cur_node.mChild.foreach {
          x =>
            val y = x.asInstanceOf[RTreeInternalEntry[D]]
            if (query.intersects(y.mbr)) q.enqueue((y.node, cur_level + 1))
        }
      }
    }
    Some(ans.toArray)
  }

  def circleRangeCnt(origin: Geometry, r: Double): Array[(Geometry, D, Int)] = {
    val ans = mutable.ArrayBuffer[(Geometry, D, Int)]()
    val st = new mutable.Stack[RTreeNode[D]]()
    if (root.mMbr.distance(origin) <= r && root.mChild.nonEmpty) st.push(root)
    while (st.nonEmpty) {
      val now = st.pop()
      if (!now.isLeaf) {
        now.mChild.foreach {
          x =>
            val y = x.asInstanceOf[RTreeInternalEntry[D]]
            if (origin.distance(y.mbr) <= r) st.push(y.node)
        }
      } else {
        now.mChild.foreach {
          x =>
            val y = x.asInstanceOf[RTreeLeafEntry[T, D]]
            if (origin.distance(y.shape) <= r) ans += ((y.shape, y.mData, y.size))
        }
      }
    }
    ans.toArray
  }

  def circleRangeConj(queries: Array[(Point, Double)]): Array[(Geometry, D)] = {

    val ans = mutable.ArrayBuffer[(Geometry, D)]()
    val st = new mutable.Stack[RTreeNode[D]]()

    def check(now: Geometry): Boolean = {
      for (i <- queries.indices)
        if (now.distance(queries(i)._1) > queries(i)._2) return false
      true
    }

    if (check(root.mMbr) && root.mChild.nonEmpty) st.push(root)
    while (st.nonEmpty) {
      val now = st.pop()
      if (!now.isLeaf) now.mChild.foreach {
        x =>
          val y = x.asInstanceOf[RTreeInternalEntry[D]]
          if (check(y.mbr)) st.push(y.node)
      } else {
        now.mChild.foreach {
          x =>
            val y = x.asInstanceOf[RTreeLeafEntry[T, D]]
            if (check(y.shape)) ans += ((y.shape, y.mData))
        }
      }
    }
    ans.toArray
  }

  def kNN(query: Point, k: Int, keepSame: Boolean = false): Array[(Geometry, D)] = {
    val ans = mutable.ArrayBuffer[(Geometry, D)]()
    val pq = new mutable.PriorityQueue[(_, Double)]()(new NNOrdering())
    var cnt = 0
    var kNN_dis = 0.0
    pq.enqueue((root, 0.0))

    val loop = new Breaks
    import loop.{break, breakable}
    breakable {
      while (pq.nonEmpty) {
        val now = pq.dequeue()
        if (cnt >= k && (!keepSame || now._2 > kNN_dis)) break()

        now._1 match {
          case RTreeNode(_, m_child, isLeaf) =>
            m_child.foreach(entry =>
              if (isLeaf) pq.enqueue((entry, entry.minDist(query)))
              else pq.enqueue((entry.asInstanceOf[RTreeInternalEntry[D]].node, entry.minDist(query)))
            )
          case x: RTreeLeafEntry[T, D] =>
            cnt += x.size
            kNN_dis = now._2
            ans += ((x.shape, x.mData))
        }
      }
    }

    ans.toArray
  }
}


object RTree {
  def apply[T <: Geometry : ClassTag, D: ClassTag](entries: Array[(T, D, Int)], maxEntriesPerNode: Int, dimension: Int = 2): RTree[T, D] = {
    val entriesLen = entries.length.toDouble
    if (entriesLen == 0) {
      val root = new RTreeNode[D](Extent(0, 0, 0, 0).toPolygon, new Array[RTreeEntry](0), false)
      new RTree(root).setNumEntries(0)
    }
    else {
      //      val dimension = entries(0)._1.getDimension
      val dim = new Array[Int](dimension)
      var remaining = entriesLen / maxEntriesPerNode
      for (i <- 0 until dimension) {
        dim(i) = Math.ceil(Math.pow(remaining, 1.0 / (dimension - i))).toInt
        remaining /= dim(i)
      }

      def compMBR(dim: Int)(left: (T, D, Int), right: (T, D, Int)): Boolean = {

        val left_center = dim match {
          case 0 => left._1.getCentroid.getCoordinate.x
          case 1 => left._1.getCentroid.getCoordinate.y
          case 2 => left._1.getUserData.asInstanceOf[Array[Double]].sum
          case _ => throw new ArithmeticException(s"dimension should be < 3. Got $dim.")
        }
        val right_center = dim match {
          case 0 => right._1.getCentroid.getCoordinate.x
          case 1 => right._1.getCentroid.getCoordinate.y
          case 2 => left._1.getUserData.asInstanceOf[Array[Double]].sum
          case _ => throw new ArithmeticException(s"dimension should be < 3. Got $dim.")
        }
        left_center < right_center
        //        left._1.intersects(right._1)
      }

      def recursiveGroupMBR(entries: Array[(T, D, Int)], curDim: Int, untilDim: Int)
      : Array[Array[(T, D, Int)]] = {
        val len = entries.length.toDouble
        val grouped = entries.sortWith(compMBR(curDim))
          .grouped(Math.ceil(len / dim(curDim)).toInt).toArray
        if (curDim < untilDim) {
          grouped.flatMap(now => recursiveGroupMBR(now, curDim + 1, untilDim))
        } else grouped
      }

      val grouped = recursiveGroupMBR(entries, 0, dimension - 1)
      val rtreeNodes = mutable.ArrayBuffer[(Polygon, RTreeNode[D])]()
      grouped.foreach(list => {
        val min = new Array[Double](dimension).map(_ => Double.MaxValue)
        val max = new Array[Double](dimension).map(_ => Double.MinValue)
        list.foreach(now => {
          //          val coords = now._1.getCoordinates.map(x => (x.x, x.y, x.z))
          val coords = now._1.getCoordinates.map(x => (x.x, x.y))
          val coordsMin = if (dimension == 2)
            Array(coords.minBy(_._1)._1, coords.minBy(_._2)._2)
          else
            Array(coords.minBy(_._1)._1, coords.minBy(_._2)._2, now._1.getUserData.asInstanceOf[Array[Double]].head.toDouble)
          val coordsMax = if (dimension == 2)
            Array(coords.maxBy(_._1)._1, coords.maxBy(_._2)._2)
          else
            Array(coords.maxBy(_._1)._1, coords.maxBy(_._2)._2, now._1.getUserData.asInstanceOf[Array[Double]](1).toDouble)
          for (i <- 0 until dimension) min(i) = Math.min(min(i), coordsMin(i))
          for (i <- 0 until dimension) max(i) = Math.max(max(i), coordsMax(i))
        })
        val mbr = Extent(min(0), min(1), max(0), max(1)).toPolygon
        if (dimension == 3) mbr.setUserData(Array(min(2), max(2)))
        rtreeNodes += ((mbr, RTreeNode[T, D](mbr, list)))
      })

      var curRtreeNodes = rtreeNodes.toArray
      var curLen = curRtreeNodes.length.toDouble
      remaining = curLen / maxEntriesPerNode
      for (i <- 0 until dimension) {
        dim(i) = Math.ceil(Math.pow(remaining, 1.0 / (dimension - i))).toInt
        remaining /= dim(i)
      }

      def over(dim: Array[Int]): Boolean = {
        for (i <- dim.indices)
          if (dim(i) != 1) return false
        true
      }

      def comp(dim: Int)(left: (Polygon, RTreeNode[D]), right: (Polygon, RTreeNode[D])): Boolean = {
        val leftCenter = dim match {
          case 0 => left._1.getCentroid.getCoordinate.x
          case 1 => left._1.getCentroid.getCoordinate.y
          case 2 => left._1.getUserData.asInstanceOf[Array[Double]].sum
          case _ => throw new ArithmeticException(s"dimension should be < 3. Got $dim.")
        }
        val rightCenter = dim match {
          case 0 => right._1.getCentroid.getCoordinate.x
          case 1 => right._1.getCentroid.getCoordinate.y
          case 2 => left._1.getUserData.asInstanceOf[Array[Double]].sum
          case _ => throw new ArithmeticException(s"dimension should be < 3. Got $dim.")
        }
        leftCenter < rightCenter
      }

      def recursiveGroupRTreeNode(entries: Array[(Polygon, RTreeNode[D])],
                                  curDim: Int, untilDim: Int): Array[Array[(Polygon, RTreeNode[D])]] = {
        val len = entries.length.toDouble
        val grouped = entries.sortWith(comp(curDim))
          .grouped(Math.ceil(len / dim(curDim)).toInt).toArray
        if (curDim < untilDim) {
          grouped.flatMap(now => recursiveGroupRTreeNode(now, curDim + 1, untilDim))
        } else grouped
      }

      while (!over(dim)) {
        val grouped = recursiveGroupRTreeNode(curRtreeNodes, 0, dimension - 1)
        val tmpNodes = mutable.ArrayBuffer[(Polygon, RTreeNode[D])]()
        grouped.foreach(list => {
          val min = new Array[Double](dimension).map(x => Double.MaxValue)
          val max = new Array[Double](dimension).map(x => Double.MinValue)
          list.foreach(now => {
            val coords = now._1.getCoordinates.map(x => (x.x, x.y))
            val coordsMin = if (dimension == 2)
              Array(coords.minBy(_._1)._1, coords.minBy(_._2)._2)
            else
              Array(coords.minBy(_._1)._1, coords.minBy(_._2)._2, now._1.getUserData.asInstanceOf[Array[Double]].head)
            val coordsMax = if (dimension == 2)
              Array(coords.maxBy(_._1)._1, coords.maxBy(_._2)._2)
            else
              Array(coords.maxBy(_._1)._1, coords.maxBy(_._2)._2, now._1.getUserData.asInstanceOf[Array[Double]](1))
            for (i <- 0 until dimension) min(i) = Math.min(min(i), coordsMin(i))
            for (i <- 0 until dimension) max(i) = Math.max(max(i), coordsMax(i))
          })
          val mbr = Extent(min(0), min(1), max(0), max(1)).toPolygon
          if (dimension == 3) mbr.setUserData(Array(min(2), max(2)))
          tmpNodes += ((mbr, RTreeNode(mbr, list)))
        })
        curRtreeNodes = tmpNodes.toArray
        curLen = curRtreeNodes.length.toDouble
        remaining = curLen / maxEntriesPerNode
        for (i <- 0 until dimension) {
          dim(i) = Math.ceil(Math.pow(remaining, 1.0 / (dimension - i))).toInt
          remaining /= dim(i)
        }
      }

      val min = new Array[Double](dimension).map(x => Double.MaxValue)
      val max = new Array[Double](dimension).map(x => Double.MinValue)
      curRtreeNodes.foreach(now => {
        val coords = now._1.getCoordinates.map(x => (x.x, x.y))
        val coordsMin = if (dimension == 2)
          Array(coords.minBy(_._1)._1, coords.minBy(_._2)._2)
        else {
          Array(coords.minBy(_._1)._1, coords.minBy(_._2)._2, now._1.getUserData.asInstanceOf[Array[Double]].head)
        }
        val coordsMax = if (dimension == 2)
          Array(coords.maxBy(_._1)._1, coords.maxBy(_._2)._2)
        else
          Array(coords.maxBy(_._1)._1, coords.maxBy(_._2)._2, now._1.getUserData.asInstanceOf[Array[Double]](1))
        for (i <- 0 until dimension) min(i) = Math.min(min(i), coordsMin(i))
        for (i <- 0 until dimension) max(i) = Math.max(max(i), coordsMax(i))
      })

      val mbr = Extent(min(0), min(1), max(0), max(1)).toPolygon
      if (dimension == 3) mbr.setUserData(Array(min(2), max(2)))
      val root = RTreeNode(mbr, curRtreeNodes)
      new RTree(root).setNumEntries(entriesLen.toInt)
    }
  }
}

