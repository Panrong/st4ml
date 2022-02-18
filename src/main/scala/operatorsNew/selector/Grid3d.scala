package operatorsNew.selector

import instances.{Duration, Extent, Instance}

/**
 * a 3-d grid structure with subclasses for partitioning
 *
 * @param xMin : min value of x dimension
 * @param xMax : max value of x dimension
 * @param yMin : min value of y dimension
 * @param yMax : max value of y dimension
 * @param tMin : min value of t dimension
 * @param tMax : max value of t dimension
 * @param sX   : the number of splits in x dimension
 * @param sY   : the number of splits in y dimension
 * @param sT   : the number of splits in t dimension
 */
class Grid3d(xMin: Double, xMax: Double, yMin: Double, yMax: Double, tMin: Long, tMax: Long,
             sX: Double, sY: Double, sT: Int) extends Serializable {
  case class Cell(xMin: Double, xMax: Double, yMin: Double, yMax: Double, tMin: Long, tMax: Long)

  /** the length of each grid in each dimension */
  val dX: Double = (xMax - xMin) / sX
  val dY: Double = (yMax - yMin) / sY
  val dT: Int = ((tMax - tMin) / sT).toInt

  val cells: Array[Cell] =
    (for (i <- (xMin to xMax by dX).sliding(2);
          j <- (yMin to yMax by dY).sliding(2);
          k <- (tMin to tMax by dT).sliding(2))
    yield Cell(i(0), i(1), j(0), j(1), k(0), k(1))).toArray

  /** find the intersection cells (and subclasses) of an instance. result e.g. 100a */
  def findIntersections[T <: Instance[_, _, _]](instance: T): Array[String] = {
    val mbr = instance.extent
    val xIdx = ((mbr.xMin - xMin) / dX).toInt to scala.math.ceil((mbr.xMax - xMin) / dX).toInt
    val yIdx = ((mbr.yMin - yMin) / dY).toInt to scala.math.ceil((mbr.yMax - yMin) / dY).toInt
    val tIdx = ((instance.duration.start - tMin) / dT).toInt to scala.math.ceil((instance.duration.end - tMin) / dT).toInt
    val totalIdx = (for (x <- xIdx; y <- yIdx; t <- tIdx) yield x * sT * sY + y * sT + t).toArray.map(_.toInt).filter(_ < cells.length)
    val idxWithClass = totalIdx.map { idx =>
      val grid = cells(idx)
      val subclass = getClass(grid, (instance.extent, instance.duration))
      idx.toString + subclass
    }
    idxWithClass
  }

  /** define the subclasses */
  def getClass(cell: Cell, mbr: (Extent, Duration)): String = {
    mbr match {
      case _ if mbr._1.xMin >= cell.xMin && mbr._1.yMin >= cell.yMin && mbr._2.start >= cell.tMin => "a"
      case _ if mbr._1.xMin >= cell.xMin && mbr._1.yMin >= cell.yMin && mbr._2.start < cell.tMin => "b"
      case _ if mbr._1.xMin >= cell.xMin && mbr._1.yMin < cell.yMin && mbr._2.start >= cell.tMin => "c"
      case _ if mbr._1.xMin >= cell.xMin && mbr._1.yMin < cell.yMin && mbr._2.start < cell.tMin => "d"
      case _ if mbr._1.xMin < cell.xMin && mbr._1.yMin >= cell.yMin && mbr._2.start >= cell.tMin => "e"
      case _ if mbr._1.xMin < cell.xMin && mbr._1.yMin >= cell.yMin && mbr._2.start < cell.tMin => "f"
      case _ if mbr._1.xMin < cell.xMin && mbr._1.yMin < cell.yMin && mbr._2.start >= cell.tMin => "g"
      case _ if mbr._1.xMin < cell.xMin && mbr._1.yMin < cell.yMin && mbr._2.start < cell.tMin => "h"
    }
  }

  /** given a query ST range, find the intersection cells and subclasses */
  def findPartitions(queryRange: (Extent, Duration)): Array[String] = {
    val mbr = queryRange._1
    val duration = queryRange._2
    val xIdx = ((mbr.xMin - xMin) / dX).toInt to scala.math.ceil((mbr.xMax - xMin) / dX).toInt
    val yIdx = ((mbr.yMin - yMin) / dY).toInt to scala.math.ceil((mbr.yMax - yMin) / dY).toInt
    val tIdx = ((duration.start - tMin) / dT).toInt to scala.math.ceil((duration.end - tMin) / dT).toInt
    val totalIdx = (for (x <- xIdx; y <- yIdx; t <- tIdx) yield x * sT * sY + y * sT + t).toArray.map(_.toInt).filter(_ < cells.length)
    var classes = Set("a", "b", "c", "d", "e", "f", "g", "h")
    val idxWithClass = totalIdx.flatMap { idx =>
      val grid = cells(idx)
      if (grid.xMin > mbr.xMin) classes -= ("e", "f", "g", "h")
      if (grid.yMin > mbr.yMin) classes -= ("c", "d", "g", "h")
      if (grid.tMin > duration.start) classes -= ("b", "d", "f", "h")
      classes.toArray.map(x => idx.toString + x)
    }
    idxWithClass
  }

}