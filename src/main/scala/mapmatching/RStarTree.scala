/** deprecated */
//package main.scala.mapmatching.RStarTree
//
//import scala.math.{abs, max, min}
//import Array.concat
//import main.scala.geometry.{Point, Rectangle, Shape}
//
//import scala.util.control._
//
//object RStarTree {
//  def apply(): Unit = {
//    val a = Rectangle(Point(1, 1), Point(3, 3))
//    val b = Rectangle(Point(2, 2), Point(4, 4))
//  }
//}
//
///*
//case class Point(xc: Double, yc: Double) extends Shape {
//  val x: Double = xc
//  val y: Double = yc
//
//  override def intersect(rectangle: Rectangle): Boolean = {
//    inside(rectangle)
//  }
//
//  override def inside(rectangle: Rectangle): Boolean = {
//    if (x >= rectangle.x_min && x <= rectangle.x_max && y >= rectangle.y_min && y <= rectangle.y_max) true
//    else false
//  }
//
//  override def mbr(): Array[Double] = Array(x, y, x, y)
//
//  override def dist(point: Point): Double = math.sqrt(math.pow(x - point.x, 2) + math.pow(y - point.y, 2))
//
//  override def center(): Point = this
//}
//
//case class Rectangle(bottomLeft: Point, topRight: Point) extends Shape {
//  val x_min = bottomLeft.x
//  val x_max = topRight.x
//  val y_min = bottomLeft.y
//  val y_max = topRight.y
//  val area: Double = (topRight.x - bottomLeft.x) * (topRight.y - bottomLeft.y)
//
//  def overlap(r: Rectangle): Double = {
//    val overlapX = max((x_max - x_min) + (r.x_max - r.x_min) - (max(x_max, r.x_max) - min(x_min, r.x_min)), 0)
//    val overlapY = max((y_max - y_min) + (r.y_max - r.y_min) - (max(y_max, r.y_max) - min(y_min, r.y_min)), 0)
//    overlapX * overlapY
//  }
//
//  def includeEntry(shape: Shape): Rectangle = {
//    // return a new Rectangle
//    if (shape.isInstanceOf[Point]) {
//      val point = shape.asInstanceOf[Point]
//      val new_x_min = min(point.x, this.x_min)
//      val new_x_max = max(point.x, this.x_max)
//      val new_y_min = min(point.y, this.y_min)
//      val new_y_max = max(point.y, this.y_max)
//      //println(Point(new_x_min, new_y_min), Point(new_x_max, new_y_max))
//      Rectangle(Point(new_x_min, new_y_min), Point(new_x_max, new_y_max))
//    }
//    else if (shape.isInstanceOf[Rectangle]) {
//      val rectangle = shape.asInstanceOf[Rectangle]
//      val new_x_min = min(rectangle.x_min, this.x_min)
//      val new_x_max = max(rectangle.x_max, this.x_max)
//      val new_y_min = min(rectangle.y_min, this.y_min)
//      val new_y_max = max(rectangle.y_max, this.y_max)
//      Rectangle(Point(new_x_min, new_y_min), Point(new_x_max, new_y_max))
//    }
//    else throw new IllegalArgumentException
//  }
//
//  def enlargement(shape: Shape): Double = {
//    //calculate the enlargement needed to include one entry
//    val newRectangle = this.includeEntry(shape)
//    newRectangle.area - this.area
//  }
//
//  def margin(): Double = {
//    (x_max - x_min + y_max - y_min) * 2
//  }
//
//  override def inside(rectangle: Rectangle): Boolean = {
//    if (x_min >= rectangle.x_min && y_min >= rectangle.y_min && x_max <= rectangle.x_max && y_max <= rectangle.y_max) true
//    else false
//  }
//
//  override def intersect(r: Rectangle): Boolean = {
//    if (abs((x_min + x_max) / 2 - (r.x_min + r.x_max) / 2) < ((x_max + r.x_max - x_min - r.x_min) / 2)
//      && abs((y_min + y_max) / 2 - (r.y_min + r.y_max) / 2) < ((y_max + r.y_max - y_min - r.y_min) / 2)) true
//    else false
//  }
//
//  override def mbr(): Array[Double] = Array(x_min, y_min, x_max, y_max)
//
//  override def center(): Point = Point((x_max - x_min) / 2.0, (y_min - y_max) / 2.0)
//
//  override def dist(point: Point): Double = math.sqrt(math.pow(center.x - point.x, 2) + math.pow(center.y - point.y, 2))
//
//}
//
//abstract class Shape() {
//  def inside(rectangle: Rectangle): Boolean
//
//  def intersect(rectangle: Rectangle): Boolean
//
//  def mbr(): Array[Double]
//
//  def dist(point: Point): Double
//
//  def center(): Point
//}
//
// */
//
//case class Node(m_box: Rectangle, m_child: Array[Node] = new Array[Node](0), isLeaf: Boolean) {
//  var leaf: Boolean = isLeaf
//  var child: Array[Node] = m_child
//  var mbr: Rectangle = m_box
//  var entries = new Array[Shape](0)
//  var parent: Rectangle = Rectangle(Point(0, 0), Point(-1, 1))
//
//  def level(): Int = {
//    if (this.leaf) 0
//    else {
//      child(0).level() + 1
//    }
//  }
//
//  def addChild(node: Node): Unit = {
//    this.child = this.child :+ node
//    node.parent = this.mbr
//  }
//
//  def dropChild(node: Node): Unit = {
//    val t = child.toBuffer
//    t -= node
//    child = t.toArray
//  }
//
//  def updateMBR(shape: Shape): Unit = {
//    mbr = mbr.includeEntry(shape)
//  }
//
//  def addEntry(shapes: Array[Shape]): Unit = {
//    entries = concat(entries, shapes)
//  }
//
//  def deleteEntry(shape: Shape): Unit = {
//    assert(isLeaf)
//    val t = entries.toBuffer
//    t -= shape
//    entries = t.toArray
//  }
//
//  def shrinkMBR(): Unit = {
//    if (isLeaf) {
//      var x = new Array[Double](0)
//      var y = new Array[Double](0)
//      for (e <- entries) {
//        val mbr = e.mbr
//        x = x :+ mbr(0) :+ mbr(2)
//        y = y :+ mbr(1) :+ mbr(3)
//      }
//      x = x.sorted
//      y = y.sorted
//      mbr = Rectangle(Point(x(0), y(0)), Point(x.last, y.last))
//    }
//    else {
//      var x = new Array[Double](0)
//      var y = new Array[Double](0)
//      for (e <- child) {
//        val mbr = e.mbr.mbr
//        x = x :+ mbr(0) :+ mbr(2)
//        y = y :+ mbr(1) :+ mbr(3)
//      }
//      x = x.sorted
//      y = y.sorted
//      mbr = Rectangle(Point(x(0), y(0)), Point(x.last, y.last))
//    }
//  }
//}
//
//case class RTree(r: Node, m: Int) {
//  var root = r
//  val M = m
//  val leaves = getLeaves(root, new Array[Node](0))
//
//  def getLeaves(node: Node, leaves: Array[Node] = new Array[Node](0)): Array[Node] = {
//    var l = leaves
//    if (node.isLeaf) {
//      l :+ node
//    }
//    else {
//      var sub_l = new Array[Node](0)
//      for (i <- node.child) {
//        var empty_l = new Array[Node](0)
//        sub_l = concat(sub_l, getLeaves(i, empty_l))
//      }
//      concat(l, sub_l)
//    }
//  }
//
//  def getNodes(node: Node, leaves: Array[Node] = new Array[Node](0)): Array[Node] = {
//    var l = leaves
//    l = l :+ node
//    if (!node.isLeaf) {
//      var sub_l = new Array[Node](0)
//      for (i <- node.child) {
//        var empty_l = new Array[Node](0)
//        sub_l = concat(sub_l, getLeaves(i, empty_l))
//      }
//      concat(l, sub_l)
//    }
//    else l
//  }
//
//  def getLevel(node: Node, level: Int, results: Array[Node] = new Array[Node](0)): Array[Node] = {
//    var l = results
//    if (node.level() == level) {
//      l :+ node
//    }
//    else {
//      var sub_l = new Array[Node](0)
//      for (i <- node.child) {
//        var empty_l = new Array[Node](0)
//        sub_l = concat(sub_l, getLevel(i, level, empty_l))
//      }
//      concat(l, sub_l)
//    }
//  }
//
//  def findNode(nodes: Array[Node] = Array(root), mbr: Rectangle): Node = { //not working well
//    for (node <- nodes) {
//      if (node.mbr.x_min == mbr.x_min && node.mbr.x_max == mbr.x_max && node.mbr.y_min == mbr.y_min && node.mbr.y_max == mbr.y_max) return node
//      else findNode(node.child, mbr)
//    }
//    throw new ArithmeticException("The node cannot find its parent")
//
//  }
//
//  def genTable(): (Map[Int, Node], Map[Int, Int]) = {
//    var RTreeMap: Map[Int, Node] = Map()
//    var childParent: Map[Int, Int] = Map()
//    for (i <- 0 to root.child.length - 1) {
//      RTreeMap += (i -> root.child(i))
//      childParent += (i -> -1)
//    }
//
//    def assignID(parent: Node, RTreeMap: Map[Int, Node], parentChild: Map[Int, Int] = Map()): (Map[Int, Node], Map[Int, Int]) = {
//      var r = RTreeMap
//      var pc = childParent
//      val parentKey = RTreeMap.filter(_._2 == parent).map(_._1).min
//      val filteredChild = parent.child.filter(x => x != null)
//      for ((child, i) <- filteredChild.zipWithIndex) {
//        r += ((i + (parentKey + 1) * m) -> child)
//        pc += ((i + (parentKey + 1) * m) -> parentKey)
//        if (!child.leaf) {
//          val res = assignID(child, r, pc)
//          r = r.++(res._1)
//          pc = pc.++(res._2)
//        }
//      }
//      (r, pc)
//    }
//
//    for (parent <- root.child) {
//      val r = assignID(parent, RTreeMap, childParent)
//      RTreeMap = RTreeMap.++(r._1)
//      childParent = childParent.++(r._2)
//
//    }
//    //for (k <- RTreeMap.keys) println(k, RTreeMap(k).mbr)
//    (RTreeMap, childParent)
//  }
//
//  def findAncestors(child: Node): Array[Node] = {
//    val r = genTable()
//    val RTreeMap = r._1
//    val childParent = r._2
//    if (RTreeMap.size <= m) Array(root)
//    else {
//      val childKey = RTreeMap.filter(_._2 == child).map(_._1).min
//      var ancestors = new Array[Node](0)
//      var id = childKey
//      while (id >= m) {
//        val parentKey = childParent(id)
//        ancestors = ancestors :+ RTreeMap(parentKey)
//        id = parentKey
//      }
//      //println(ancestors.deep)
//      ancestors :+ root
//    }
//  }
//
//  def leafEntries(): Map[Int, Array[Shape]] = {
//    var entriesMap: Map[Int, Array[Shape]] = Map()
//    val r = genTable()._1
//    for (leaf <- getLeaves(root)) {
//      if (leaf.level() == 0) {
//        var entries = new Array[Shape](0)
//        val id = r.filter(_._2 == leaf).map(_._1).max
//        for (e <- leaf.entries) entries = entries :+ e
//        entriesMap += (id -> entries)
//      }
//    }
//    entriesMap
//  }
//}
//
//
//object RTree {
//  def apply(entries: Array[(Point, Int)], max_entries_per_node: Int): Unit = {
//  }
//
//  def chooseSubtree(shape: Shape, rtree: RTree, node: Node): Node = { // return the node to insert the entry
//    var s = shape
//    if (node.isLeaf) node
//    else {
//      if (node.child(0).leaf) { // select the child with min overlap
//        var enlargement = new Array[Array[Double]](0)
//        // calculate enlargement
//        var i = 0
//        for (box <- node.child) {
//          enlargement = enlargement :+ Array(i, box.mbr.enlargement(s))
//          i += 1
//        }
//        enlargement = enlargement.sortBy(_ (1)).take(32) //32 according to original paper
//        var enlargement_t = enlargement.transpose
//        var indices = enlargement_t(0).map(_.toInt).sorted
//        val boxes = indices collect node.child
//        //calculate overlap
//        var overlaps = new Array[Array[Double]](0)
//        i = 0
//        for (box <- boxes) {
//          var new_box = box.mbr.includeEntry(s)
//          var overlap: Double = 0
//          for (otherBox <- boxes) {
//            if (box != otherBox) overlap += new_box.overlap(otherBox.mbr)
//          }
//          overlaps = overlaps :+ Array(i, overlap)
//          //println(box.mbr, overlap)
//          i += 1
//        }
//        overlaps = overlaps.sortBy(_ (1))
//        node.child(overlaps(0)(0).toInt)
//      }
//      else { // select the child with min enlargement
//        var enlargement = new Array[Array[Double]](0)
//        // calculate enlargement
//        var i = 0
//        for (box <- node.child) {
//          enlargement = enlargement :+ Array(i, box.mbr.enlargement(s))
//          i += 1
//        }
//        enlargement = enlargement.sortBy(_ (1)).take(1)
//        node.child(enlargement(0)(0).toInt)
//      }
//    }
//  }
//
//  def insert(shape: Shape, rtree: RTree, node: Node): Unit = {
//    node.updateMBR(shape)
//    val nextNode = chooseSubtree(shape, rtree, node)
//    if (!node.leaf) {
//      insert(shape, rtree, nextNode)
//    }
//    else {
//      node.entries = node.entries :+ shape
//    }
//  }
//
//  def splitSubtree(node: Node, m_frac: Double = 0.4): Array[Node] = {
//    var M: Int = 0
//    if (node.isLeaf) M = node.entries.length - 1
//    else M = node.child.length - 1
//    val m = math.ceil(M * m_frac).toInt
//
//    def chooseSplitAxis(node: Node): Int = { // return 0 for x axis and 1 for y axis to split first
//      var boxes = getChildBoxes(node)
//      boxes = boxes.sortBy(r => (r.x_min, r.x_max))
//      val s1 = getMargin(boxes, M, m)
//      boxes = boxes.sortBy(r => (r.y_min, r.y_max))
//      val s2 = getMargin(boxes, M, m)
//      if (s1 < s2) 0
//      else 1
//    }
//
//    def chooseSplitIndex(node: Node, axis: Int): Int = { // return axis, index
//      var boxes = getChildBoxes(node)
//      if (axis == 0) boxes = boxes.sortBy(r => (r.x_min, r.x_max))
//      else boxes = boxes.sortBy(r => (r.y_min, r.y_max))
//      val idx = getOverlap(boxes, M, m)
//      idx
//    }
//
//    def split(node: Node, info: (Int, Int)): Array[Node] = {
//      val k = info._2
//      var boxes = getChildBoxes(node)
//      if (info._1 == 0) boxes = boxes.sortBy(r => (r.x_min, r.x_max))
//      else boxes = boxes.sortBy(r => (r.y_min, r.y_max))
//      var group1 = boxes.take(m - 1 + k)
//      var group2 = boxes.drop(m - 1 + k)
//      val bb1 = getBoundingBox(group1)
//      val bb2 = getBoundingBox(group2)
//
//      var child1 = new Array[Node](0)
//      var child2 = new Array[Node](0)
//      var entry1 = new Array[Shape](0)
//      var entry2 = new Array[Shape](0)
//      if (node.isLeaf) {
//        for (i <- node.entries) {
//          if (i.inside(bb1) && (!i.inside(bb2)) && (!bb1.inside(bb2))) entry1 = entry1 :+ i
//          else entry2 = entry2 :+ i
//        }
//      }
//      else {
//        for (i <- node.child) {
//          if (i.mbr.inside(bb1) && (!i.mbr.inside(bb2)) && (!bb1.inside(bb2))) child1 = child1 :+ i
//          else child2 = child2 :+ i
//        }
//      }
//      val node1 = Node(bb1, child1, node.isLeaf)
//      node1.addEntry(entry1)
//      node1.parent = node.parent
//      val node2 = Node(bb2, child2, node.isLeaf)
//      node2.addEntry(entry2)
//      node2.parent = node.parent
//
//      Array(node1, node2)
//    }
//
//    def getChildBoxes(node: Node): Array[Rectangle] = {
//      var boxes = new Array[Rectangle](0)
//      if (node.leaf) {
//        for (i <- node.entries) {
//          if (i.isInstanceOf[Point]) {
//            var p = i.asInstanceOf[Point]
//            boxes = boxes :+ Rectangle(p, p)
//          }
//          else {
//            var r = i.asInstanceOf[Rectangle]
//            boxes = boxes :+ r
//          }
//        }
//      }
//      else {
//        for (i <- node.child) {
//          boxes = boxes :+ i.mbr
//        }
//      }
//      boxes
//    }
//
//    def getMargin(boxes: Array[Rectangle], M: Int, m: Int): Double = {
//      var s: Double = 0
//      for (k <- 1 to M - 2 * m + 2) {
//        var group1 = boxes.take(m - 1 + k)
//        var group2 = boxes.drop(m - 1 + k)
//        var bb1 = getBoundingBox(group1)
//        var bb2 = getBoundingBox(group2)
//        s += bb1.margin() + bb2.margin()
//      }
//      s
//    }
//
//    def getOverlap(boxes: Array[Rectangle], M: Int, m: Int): Int = {
//      var s: Map[Int, Double] = Map()
//      for (k <- 1 to M - 2 * m + 2) {
//        var group1 = boxes.take(m - 1 + k)
//        var group2 = boxes.drop(m - 1 + k)
//        var bb1 = getBoundingBox(group1)
//        var bb2 = getBoundingBox(group2)
//        s += (k -> bb1.overlap(bb2))
//      }
//      val r = s.min
//      r._1
//    }
//
//    def getBoundingBox(rectangles: Array[Rectangle]): Rectangle = {
//      var x_mins = new Array[Double](0)
//      var y_mins = new Array[Double](0)
//      var x_maxs = new Array[Double](0)
//      var y_maxs = new Array[Double](0)
//      for (i <- rectangles) {
//        x_mins = x_mins :+ i.x_min
//        x_maxs = x_maxs :+ i.x_max
//        y_mins = y_mins :+ i.y_min
//        y_maxs = y_maxs :+ i.y_max
//      }
//      Rectangle(Point(x_mins.reduceLeft(_ min _), y_mins.reduceLeft(_ min _)), Point(x_maxs.reduceLeft(_ max _), y_maxs.reduceLeft(_ max _)))
//    }
//
//    def getBoundingBoxP(points: Array[Point]): Rectangle = {
//      var xs = new Array[Double](0)
//      var ys = new Array[Double](0)
//      for (i <- points) {
//        xs = xs :+ i.x
//        ys = ys :+ i.y
//      }
//      Rectangle(Point(xs.reduceLeft(_ min _), ys.reduceLeft(_ min _)), Point(xs.reduceLeft(_ max _), ys.reduceLeft(_ max _)))
//    }
//
//    val axis = chooseSplitAxis(node)
//    val idx = chooseSplitIndex(node, axis)
//    //println(axis,idx)
//    val new_nodes = split(node, (axis, idx))
//    //for (i <- new_nodes) println(i.mbr)
//    new_nodes
//  }
//
//  def replaceChild(parent: Node, child: Node, M: Int) = {
//    // if (child.isLeaf) println(child.entries.length)
//    if ((child.isLeaf && child.entries.length > M) || ((!child.isLeaf) && child.child.length > M)) {
//      val new_nodes = splitSubtree(child)
//      parent.dropChild(child)
//      for (n <- new_nodes) {
//        parent.addChild(n)
//      }
//    }
//    else if (((!child.leaf) && child.child.length == 0) || (child.leaf && child.entries.length == 0)) {
//      parent.dropChild(child)
//      parent.shrinkMBR()
//    }
//  }
//
//  def findAncestor(root: Node, leaf: Node): Array[Node] = {
//    var ancestors = new Array[Node](0)
//
//    def find(parent: Node, child: Node): Unit = {
//      if (!parent.isLeaf) {
//        val p_box = parent.mbr
//        val c_box = child.mbr
//
//        if (c_box.inside(p_box)) {
//          ancestors = ancestors :+ parent
//          for (p <- parent.child) {
//            find(p, child)
//          }
//        }
//      }
//    }
//
//    find(root, leaf)
//    ancestors.reverse
//  }
//
//  def checkBalance(rtree: RTree, child: Node, M: Int) = { //node: after insertion, the leaf node to put the new object
//
//    if (rtree.root.child.length > M || (rtree.root.isLeaf && rtree.root.entries.length > M)) {
//      val new_root = Node(rtree.root.mbr, Array(rtree.root), false)
//      rtree.root = new_root
//      //RTree.printTree(rtree.root)
//    }
//    //val ancestors = findAncestor(rtree.root, child)
//    val ancestors = rtree.findAncestors(child)
//
//    val nodes = child +: ancestors
//    for (n <- 0 to nodes.length - 2) {
//      replaceChild(nodes(n + 1), nodes(n), M)
//    }
//  }
//
//  def insertEntry(shape: Shape, rtree: RTree) = {
//    insert(shape, rtree, rtree.root)
//    val l = rtree.getLeaves(rtree.root, new Array[Node](0))
//    for (i <- l) {
//      checkBalance(rtree, i, rtree.M)
//    }
//  }
//
//  def printTree(node: Node): Unit = {
//    if (node != null) println(node.mbr.x_min, node.mbr.y_min, node.mbr.x_max, node.mbr.y_max, node.level())
//    for (i <- node.child) {
//      if (i != null) printTree(i)
//    }
//  }
//
//  def rangeQuery(rectangle: Rectangle, node: Node, entries: Array[Shape] = new Array[Shape](0)): Array[Shape] = {
//    var ne = entries
//    if (node.isLeaf) {
//      for (e <- node.entries) {
//        if (e.intersect(rectangle)) {
//          ne = ne :+ e
//        }
//      }
//    }
//    else {
//      for (n <- node.child) {
//        var new_entries = new Array[Shape](0)
//        if (n.mbr.intersect(rectangle)) {
//          var e = rangeQuery(rectangle, n, new_entries)
//          new_entries = concat(new_entries, e)
//        }
//        ne = concat(ne, new_entries)
//      }
//    }
//    ne
//  }
//
//  def delete(entries: Array[Shape], rtree: RTree): RTree = {
//    var leaves = new Array[Node](0)
//    for (entry <- entries) {
//      val leaf = locateEntry(entry, rtree.root)
//      leaf.deleteEntry(entry)
//      leaves = leaves :+ leaf
//    }
//    condenseTree(leaves.distinct, rtree)
//  }
//
//  def findLeaf(entry: Shape, node: Node, leafNode: Array[Node] = new Array[Node](0)): Array[Node] = {
//    var ln = leafNode
//    if (node.isLeaf) {
//      leafNode :+ node
//    }
//    else {
//      for (n <- node.child) {
//        if (entry.intersect(n.mbr)) {
//          var new_ln = new Array[Node](0)
//          new_ln = concat(new_ln, findLeaf(entry, n))
//          ln = concat(ln, new_ln)
//        }
//      }
//      ln
//    }
//  }
//
//  def locateEntry(entry: Shape, node: Node): Node = { // find the leaf storing an entry
//    val leafNode = findLeaf(entry, node)
//    for (n <- leafNode) {
//      for (e <- n.entries) {
//        if (entry.mbr.deep == e.mbr.deep) return n
//      }
//    }
//    throw new ArithmeticException("The entry is not found in the tree")
//  }
//
//  def condenseTree(nodes: Array[Node], rtree: RTree, m: Double = 0.3): RTree = {
//    var minEntry = (m * rtree.M).toInt
//    var Q = new Array[Shape](0)
//    for (node <- nodes) {
//      for (n <- node +: findAncestor(rtree.root, node).reverse) {
//        //for (n <- node +: findAncestor(rtree, node).reverse) {
//        n.shrinkMBR()
//      }
//      if (node.entries.length < minEntry) {
//        Q = concat(Q, node.entries)
//        node.entries = new Array[Shape](0)
//        checkBalance(rtree, node, rtree.M)
//      }
//    }
//    for (entry <- Q) RTree.insertEntry(entry, rtree)
//
//    rtree
//
//  }
//
//  /*
//  def forcedReinsert(entry: Shape, rtree: RTree): RTree = {
//    def insertData(entry: Shape, rtree: RTree) = {
//      var node = rtree.root
//      val leaves = rtree.getLeaves(rtree.root)
//      var overflow: Map[Int, Boolean] = Map()
//      val rootLevel = node.level()
//      for(l <- 0 to rootLevel) overflow += (l -> false)
//
//      insert(entry, 0, overflow)
//    }
//
//    def chooseSubtreeLevel(entry: Shape, rtree: RTree, level: Int): Node = {
//      var node = rtree.root
//      val rootLevel = node.level()
//      for (l <- rootLevel to level by -1) {
//        node.updateMBR(entry)
//        val nextNode = chooseSubtree(entry, rtree, node)
//        node = nextNode
//      }
//      node
//    }
//
//    def insert(entry: Shape, level: Int = 0, rtree:RTree, reinsert:Boolean = true): Unit = {
//      val node = chooseSubtreeLevel(entry, level)
//      if (node.entries.length < rtree.M) node.addEntry(Array(entry))
//      else {
//        val newNodes = overflowTreatment(entry, node, reinsert)
//        if (newNodes.length == 2) {
//          val nodes = findAncestor(newNodes(0), rtree.root).reverse
//          nodes(0).dropChild(node)
//          for (n <- newNodes) nodes(0).addChild(n)
//          for (i <- 0 to nodes.length - 2) {
//            val new_nodes = overflowTreatmentBackPropagate(nodes(i), nodes(i + 1), reinsert)
//            if (new_nodes.length == 2) {
//              val nodes = findAncestor(newNodes(0), rtree.root).reverse
//              nodes(0).dropChild(node)
//              for (n <- newNodes) nodes(0).addChild(n)
//            }
//          }
//        }
//      }
//    }
//
//
//    def overflowTreatment(entry: Shape, node: Node, ri: Boolean = false): Array[Node] = {
//      if(entry != rtree.root && ri) Array(reinsert(node, rtree))
//      else {
//        splitSubtree(node)
//      }
//    }
//
//    def overflowTreatmentBackPropagate(child: Node, parent: Node, ri: Boolean= false) = {
//      if (ri) Array(reinsert(child, rtree))
//      else splitSubtree(child)
//      }
//
//
//    def reinsert(node: Node, rtree: RTree, p: Double = 0.3): Node = {
//      if (node.isLeaf) {
//        var dist: Map[Shape, Double] = Map()
//        val center = node.mbr.center
//        for (e <- node.entries) {
//          dist += (e -> center.dist(e.center))
//        }
//        var sortedDist = dist.toList.sortBy(-_._2)
//        val toInsert = sortedDist.take((p * rtree.M).toInt)
//        sortedDist = sortedDist.drop((p * rtree.M).toInt)
//        node.shrinkMBR()
//        for (n <- toInsert) {
//          insert(n._1, node.level(),false)
//        }
//      }
//      else {
//        var dist: Map[Node, Double] = Map()
//        val center = node.mbr.center
//        for (e <- node.child) {
//          dist += (e -> center.dist(e.mbr.center))
//        }
//        var sortedDist = dist.toList.sortBy(-_._2)
//        val toInsert = sortedDist.take((p * rtree.M).toInt)
//        sortedDist = sortedDist.drop((p * rtree.M).toInt)
//        node.shrinkMBR()
//        for (n <- toInsert) {
//          insert(n._1, node.level(),false)
//        }
//      }
//      node
//    }
//
//  }
//*/
//}
//
//object queryWithTable {
//  def apply(table: Map[Int, Rectangle], entries: Map[Int, Array[Shape]], capacity: Int, qRange: Rectangle): Array[Shape] = {
//    //table: all nodes and their mbr, entries: leaf nodes and their entries
//    val keyArray = table.keys.toArray
//    val maxKey = keyArray.max
//
//    def findLeaf(parent: Int, qRange: Rectangle, capacity: Int, children: Array[Int], leafThreshold: Int): Array[Int] = {
//      var selectedChildren = children
//      for (child <- (parent + 1) * capacity to (parent + 2) * capacity - 1) {
//        if (keyArray.contains(child) && qRange.intersect(table(child))) {
//          if (child > leafThreshold) selectedChildren = selectedChildren :+ child
//          else selectedChildren = Array.concat(selectedChildren, findLeaf(child, qRange, capacity, selectedChildren, leafThreshold))
//        }
//      }
//      selectedChildren
//    }
//
//    val children = findLeaf(-1, qRange, capacity, new Array[Int](0), maxKey / capacity - 1)
//    var retrievedEntries = new Array[Shape](0)
//    for (k <- children) {
//      for (e <- entries(k)) {
//        if (e.intersect(qRange)) retrievedEntries = retrievedEntries :+ e
//      }
//    }
//    retrievedEntries
//  }
//}
