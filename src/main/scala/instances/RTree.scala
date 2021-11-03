//package instances
//
//import scala.collection.mutable
//import scala.util.control.Breaks
//import scala.reflect.ClassTag
//
//abstract class RTreeEntry {
//  def minDist(x: Geometry): Double
//
//  def intersect(x: Geometry): Boolean
//}
//
//case class RTreeLeafEntry[T <: Geometry : ClassTag](shape: T, m_data: String, size: Int) extends RTreeEntry {
//  override def minDist(x: Geometry): Double = shape.distance(x)
//
//  override def intersect(x: Geometry): Boolean = x.intersects(shape)
//}
//
//case class RTreeInternalEntry(mbr: Extent, node: RTreeNode) extends RTreeEntry {
//  override def minDist(x: Geometry): Double = mbr.distance(x)
//
//  override def intersect(x: Geometry): Boolean = x.intersects(mbr)
//}
//
//case class RTreeNode(m_mbr: Extent, m_child: Array[RTreeEntry], isLeaf: Boolean) {
//  val size: Long = {
//    if (isLeaf) m_child.map(x => x.asInstanceOf[RTreeLeafEntry[Geometry]].size).sum
//    else m_child.map(x => x.asInstanceOf[RTreeInternalEntry].node.size).sum
//  }
//}
//
//object RTreeNode {
//  def apply(m_mbr: Extent, children: Array[(Extent, RTreeNode)]): RTreeNode = {
//    RTreeNode(m_mbr, children.map(x => RTreeInternalEntry(x._1, x._2)), isLeaf = false)
//  }
//
//  def apply[T <: Geometry : ClassTag](m_mbr: Extent, children: => Array[(T, String)]): RTreeNode = {
//    RTreeNode(m_mbr, children.map(x => RTreeLeafEntry(x._1, x._2, 1)), isLeaf = true)
//  }
//
//  def apply[T <: Geometry : ClassTag](m_mbr: Extent, children: Array[(T, String, Int)]): RTreeNode = {
//    RTreeNode(m_mbr, children.map(x => RTreeLeafEntry(x._1, x._2, x._3)), isLeaf = true)
//  }
//}
//
//class NNOrdering() extends Ordering[(_, Double)] {
//  def compare(a: (_, Double), b: (_, Double)): Int = -a._2.compare(b._2)
//}
//
//case class RTree[T <: Geometry](root: RTreeNode) extends Serializable {
//  var numEntries: Int = 0
//
//  def setNumEntries(n: Int): RTree[T] = {
//    numEntries = n
//    this
//  }
//
//  def range(query: Polygon): Array[(T, String)] = {
//    val ans = mutable.ArrayBuffer[(T, String)]()
//    val st = new mutable.Stack[RTreeNode]()
//    if (root.m_mbr.intersects(query) && root.m_child.nonEmpty) st.push(root)
//    while (st.nonEmpty) {
//      val now = st.pop()
//      if (!now.isLeaf) {
//        now.m_child.foreach {
//          case RTreeInternalEntry(mbr, node) =>
//            if (query.intersects(mbr)) st.push(node)
//        }
//      } else {
//        now.m_child.foreach {
//          case RTreeLeafEntry(shape: T, m_data, _) =>
//            if (query.intersects(shape)) ans += ((shape, m_data))
//        }
//      }
//    }
//    ans.toArray
//  }
//
//  def range(query: Polygon, level_limit: Int, s_threshold: Double): Option[Array[(T, String)]] = {
//    val ans = mutable.ArrayBuffer[(T, String)]()
//    val q = new mutable.Queue[(RTreeNode, Int)]()
//    if (root.m_mbr.intersects(query) && root.m_child.nonEmpty) q.enqueue((root, 1))
//    var estimate: Double = 0
//    val loop = new Breaks
//    import loop.{break, breakable}
//    breakable {
//      while (q.nonEmpty) {
//        val now = q.dequeue
//        val cur_node = now._1
//        val cur_level = now._2
//        if (cur_node.isLeaf) {
//          cur_node.m_child.foreach {
//            case RTreeLeafEntry(shape: T, m_data, _) =>
//              if (query.intersects(shape)) ans += ((shape, m_data))
//          }
//        } else if (cur_level < level_limit) {
//          cur_node.m_child.foreach {
//            case RTreeInternalEntry(mbr, node) =>
//              if (query.intersects(mbr)) q.enqueue((node, cur_level + 1))
//          }
//        } else if (cur_level == level_limit) {
//
//          val overlappingRatio = {
//            try cur_node.m_mbr.intersection(query).getArea / cur_node.m_mbr.area
//            catch {
//              case _: Throwable => 0.0
//            }
//          }
//          estimate += overlappingRatio * cur_node.size
//          cur_node.m_child.foreach {
//            case RTreeInternalEntry(mbr, node) =>
//              if (query.intersects(mbr)) q.enqueue((node, cur_level + 1))
//          }
//        } else break
//      }
//    }
//    if (ans.nonEmpty) return Some(ans.toArray)
//    else if (estimate / root.size > s_threshold) return None
//    while (q.nonEmpty) {
//      val now = q.dequeue
//      val cur_node = now._1
//      val cur_level = now._2
//      if (cur_node.isLeaf) {
//        cur_node.m_child.foreach {
//          case RTreeLeafEntry(shape: T, m_data, _) =>
//            if (query.intersects(shape)) ans += ((shape, m_data))
//        }
//      } else {
//        cur_node.m_child.foreach {
//          case RTreeInternalEntry(mbr, node) =>
//            if (query.intersects(mbr)) q.enqueue((node, cur_level + 1))
//        }
//      }
//    }
//    Some(ans.toArray)
//  }
//
//  def circleRangeCnt(origin: Geometry, r: Double): Array[(Geometry, String, Int)] = {
//    val ans = mutable.ArrayBuffer[(Geometry, String, Int)]()
//    val st = new mutable.Stack[RTreeNode]()
//    if (root.m_mbr.distance(origin) <= r && root.m_child.nonEmpty) st.push(root)
//    while (st.nonEmpty) {
//      val now = st.pop()
//      if (!now.isLeaf) {
//        now.m_child.foreach {
//          case RTreeInternalEntry(mbr, node) =>
//            if (origin.distance(mbr) <= r) st.push(node)
//        }
//      } else {
//        now.m_child.foreach {
//          case RTreeLeafEntry(shape, m_data, size) =>
//            if (origin.distance(shape) <= r) ans += ((shape, m_data, size))
//        }
//      }
//    }
//    ans.toArray
//  }
//
//  def circleRangeConj(queries: Array[(Point, Double)]): Array[(Geometry, String)] = {
//
//    val ans = mutable.ArrayBuffer[(Geometry, String)]()
//    val st = new mutable.Stack[RTreeNode]()
//
//    def check(now: Geometry): Boolean = {
//      for (i <- queries.indices)
//        if (now.distance(queries(i)._1) > queries(i)._2) return false
//      true
//    }
//
//    if (check(root.m_mbr) && root.m_child.nonEmpty) st.push(root)
//    while (st.nonEmpty) {
//      val now = st.pop()
//      if (!now.isLeaf) now.m_child.foreach {
//        case RTreeInternalEntry(mbr, node) =>
//          if (check(mbr)) st.push(node)
//      } else {
//        now.m_child.foreach {
//          case RTreeLeafEntry(shape, m_data, _) =>
//            if (check(shape)) ans += ((shape, m_data))
//        }
//      }
//    }
//    ans.toArray
//  }
//
//  def kNN(query: Point, k: Int, keepSame: Boolean = false): Array[(Geometry, String)] = {
//    val ans = mutable.ArrayBuffer[(Geometry, String)]()
//    val pq = new mutable.PriorityQueue[(_, Double)]()(new NNOrdering())
//    var cnt = 0
//    var kNN_dis = 0.0
//    pq.enqueue((root, 0.0))
//
//    val loop = new Breaks
//    import loop.{break, breakable}
//    breakable {
//      while (pq.nonEmpty) {
//        val now = pq.dequeue()
//        if (cnt >= k && (!keepSame || now._2 > kNN_dis)) break()
//
//        now._1 match {
//          case RTreeNode(_, m_child, isLeaf) =>
//            m_child.foreach(entry =>
//              if (isLeaf) pq.enqueue((entry, entry.minDist(query)))
//              else pq.enqueue((entry.asInstanceOf[RTreeInternalEntry].node, entry.minDist(query)))
//            )
//          case RTreeLeafEntry(p, m_data, size) =>
//            cnt += size
//            kNN_dis = now._2
//            ans += ((p, m_data))
//        }
//      }
//    }
//
//    ans.toArray
//  }
//}
//
//
//object RTree {
//  def apply[T <: Geometry : ClassTag](entries: Array[(T, String, Int)], max_entries_per_node: Int): RTree[T] = {
//    val entries_len = entries.length.toDouble
//    if (entries_len == 0) {
//      val root = new RTreeNode(Extent(0, 0, 0, 0), new Array[RTreeEntry](0), false)
//      new RTree(root).setNumEntries(0)
//    }
//    else {
//      val dimension = entries(0)._1.getDimension
//      val dim = new Array[Int](dimension)
//      var remaining = entries_len / max_entries_per_node
//      for (i <- 0 until dimension) {
//        dim(i) = Math.ceil(Math.pow(remaining, 1.0 / (dimension - i))).toInt
//        remaining /= dim(i)
//      }
//
//      def compMBR(dim: Int)(left: (T, String, Int), right: (T, String, Int)): Boolean = {
//
//        val left_center = dim match {
//          case 0 => left._1.getCentroid.getCoordinate.x
//          case 1 => left._1.getCentroid.getCoordinate.y
//          case 2 => left._1.getCentroid.getCoordinate.z
//          case _ => throw new ArithmeticException(s"dimension should be < 3. Got $dim.")
//        }
//        val right_center = dim match {
//          case 0 => right._1.getCentroid.getCoordinate.x
//          case 1 => right._1.getCentroid.getCoordinate.y
//          case 2 => right._1.getCentroid.getCoordinate.z
//          case _ => throw new ArithmeticException(s"dimension should be < 3. Got $dim.")
//        }
//        left_center < right_center
//      }
//
//      def recursiveGroupMBR(entries: Array[(T, String, Int)], cur_dim: Int, until_dim: Int)
//      : Array[Array[(T, String, Int)]] = {
//        val len = entries.length.toDouble
//        val grouped = entries.sortWith(compMBR(cur_dim))
//          .grouped(Math.ceil(len / dim(cur_dim)).toInt).toArray
//        if (cur_dim < until_dim) {
//          grouped.flatMap(now => recursiveGroupMBR(now, cur_dim + 1, until_dim))
//        } else grouped
//      }
//
//      val grouped = recursiveGroupMBR(entries, 0, dimension - 1)
//      val rtree_nodes = mutable.ArrayBuffer[(Extent, RTreeNode)]()
//      grouped.foreach(list => {
//        val min = new Array[Double](dimension).map(_ => Double.MaxValue)
//        val max = new Array[Double](dimension).map(_ => Double.MinValue)
//        list.foreach(now => {
//          val coords = now._1.getCoordinates.map(x => (x.x, x.y, x.z))
//          val coordsMin = Array(coords.minBy(_._1)._1, coords.minBy(_._2)._2, coords.minBy(_._3)._3)
//          val coordsMax = Array(coords.maxBy(_._1)._1, coords.maxBy(_._2)._2, coords.maxBy(_._3)._3)
//          for (i <- 0 until dimension) min(i) = Math.min(min(i), coordsMin(i))
//          for (i <- 0 until dimension) max(i) = Math.max(max(i), coordsMax(i))
//        })
//        val mbr = Extent(min ++ max)
//        rtree_nodes += ((mbr, RTreeNode(mbr, list)))
//      })
//
//      var cur_rtree_nodes = rtree_nodes.toArray
//      var cur_len = cur_rtree_nodes.length.toDouble
//      remaining = cur_len / max_entries_per_node
//      for (i <- 0 until dimension) {
//        dim(i) = Math.ceil(Math.pow(remaining, 1.0 / (dimension - i))).toInt
//        remaining /= dim(i)
//      }
//
//      def over(dim: Array[Int]): Boolean = {
//        for (i <- dim.indices)
//          if (dim(i) != 1) return false
//        true
//      }
//
//      def comp(dim: Int)(left: (Extent, RTreeNode), right: (Extent, RTreeNode)): Boolean = {
//        val left_center = dim match {
//          case 1 => left._1.toPolygon.getCentroid.getCoordinate.x
//          case 2 => left._1.getCentroid.getCoordinate.y
//          case 3 => left._1.getCentroid.getCoordinate.z
//          case _ => throw new ArithmeticException(s"dimension should be <= 3. Got $dim.")
//        }
//        val right_center = dim match {
//          case 1 => right._1.getCentroid.getCoordinate.x
//          case 2 => right._1.getCentroid.getCoordinate.y
//          case 3 => right._1.getCentroid.getCoordinate.z
//          case _ => throw new ArithmeticException(s"dimension should be <= 3. Got $dim.")
//        }
//        left_center < right_center
//      }
//
//      def recursiveGroupRTreeNode(entries: Array[(Extent, RTreeNode)],
//                                  cur_dim: Int, until_dim: Int): Array[Array[(Extent, RTreeNode)]] = {
//        val len = entries.length.toDouble
//        val grouped = entries.sortWith(comp(cur_dim))
//          .grouped(Math.ceil(len / dim(cur_dim)).toInt).toArray
//        if (cur_dim < until_dim) {
//          grouped.flatMap(now => recursiveGroupRTreeNode(now, cur_dim + 1, until_dim))
//        } else grouped
//      }
//
//      while (!over(dim)) {
//        val grouped = recursiveGroupRTreeNode(cur_rtree_nodes, 0, dimension - 1)
//        var tmp_nodes = mutable.ArrayBuffer[(Extent, RTreeNode)]()
//        grouped.foreach(list => {
//          val min = new Array[Double](dimension).map(x => Double.MaxValue)
//          val max = new Array[Double](dimension).map(x => Double.MinValue)
//          list.foreach(now => {
//            val coords = now._1.getCoordinates.map(x => (x.x, x.y, x.z))
//            val coordsMin = Array(coords.minBy(_._1)._1, coords.minBy(_._2)._2, coords.minBy(_._3)._3)
//            val coordsMax = Array(coords.maxBy(_._1)._1, coords.maxBy(_._2)._2, coords.maxBy(_._3)._3)
//            for (i <- 0 until dimension) min(i) = Math.min(min(i), coordsMin(i))
//            for (i <- 0 until dimension) max(i) = Math.max(max(i), coordsMax(i))
//          })
//          val mbr = Extent(min ++ max)
//          tmp_nodes += ((mbr, RTreeNode(mbr, list)))
//        })
//        cur_rtree_nodes = tmp_nodes.toArray
//        cur_len = cur_rtree_nodes.length.toDouble
//        remaining = cur_len / max_entries_per_node
//        for (i <- 0 until dimension) {
//          dim(i) = Math.ceil(Math.pow(remaining, 1.0 / (dimension - i))).toInt
//          remaining /= dim(i)
//        }
//      }
//
//      val min = new Array[Double](dimension).map(x => Double.MaxValue)
//      val max = new Array[Double](dimension).map(x => Double.MinValue)
//      cur_rtree_nodes.foreach(now => {
//        val coords = now._1.getCoordinates.map(x => (x.x, x.y, x.z))
//        val coordsMin = Array(coords.minBy(_._1)._1, coords.minBy(_._2)._2, coords.minBy(_._3)._3)
//        val coordsMax = Array(coords.maxBy(_._1)._1, coords.maxBy(_._2)._2, coords.maxBy(_._3)._3)
//        for (i <- 0 until dimension) min(i) = Math.min(min(i), coordsMin(i))
//        for (i <- 0 until dimension) max(i) = Math.max(max(i), coordsMax(i))
//      })
//
//      val mbr = Extent(min ++ max)
//      val root = RTreeNode(mbr, cur_rtree_nodes)
//      new RTree(root).setNumEntries(entries_len.toInt)
//    }
//  }
//}
//
