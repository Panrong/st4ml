package index

import geometry.Shape

abstract class RTreeEntry {
  def minDist(x: Shape): Double
  def intersects(x: Shape): Boolean
}

case class RTreeLeafEntry(shape: Shape, m_data: Int, size: Int) extends RTreeEntry {
  override def minDist(x: Shape): Double = shape.minDist(x)
  override def intersects(x: Shape): Boolean = x.intersects(shape)
}

case class RTreeInternalEntry(mbr: MBR, node: RTreeNode) extends RTreeEntry {
  override def minDist(x: Shape): Double = mbr.minDist(x)
  override def intersects(x: Shape): Boolean = x.intersects(mbr)
}

case class RTreeNode(m_mbr: MBR, m_child: Array[RTreeEntry], isLeaf: Boolean) {
  def this(m_mbr: MBR, children: Array[(MBR, RTreeNode)]) = {
    this(m_mbr, children.map(x => RTreeInternalEntry(x._1, x._2)), false)
  }

  // XX Interesting Trick! Overriding same function
  def this(m_mbr: MBR, children: => Array[(Point, Int)]) = {
    this(m_mbr, children.map(x => RTreeLeafEntry(x._1, x._2, 1)), true)
  }

  def this(m_mbr: MBR, children: Array[(MBR, Int, Int)]) = {
    this(m_mbr, children.map(x => RTreeLeafEntry(x._1, x._2, x._3)), true)
  }

  val size: Long = {
    if (isLeaf) m_child.map(x => x.asInstanceOf[RTreeLeafEntry].size).sum
    else m_child.map(x => x.asInstanceOf[RTreeInternalEntry].node.size).sum
  }
}

class RTree {

}
