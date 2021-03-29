package geometry

import scala.reflect.ClassTag

/**
 * A spatialMap records the information of a spatial range at a certain time stamp.
 *
 * @param id        : id of the spatialMap
 * @param timeStamp : the temporal interval of this SpatialMap
 * @param contents  : a rectangle recording its spatial region and some other information
 * @tparam T : Type of contents
 */
case class SpatialMap[T: ClassTag](id: String, timeStamp: (Long, Long), contents: Array[(Rectangle, Array[T])]) {

  def startTime: Long = timeStamp._1

  def endTime: Long = timeStamp._2

  def add(sm: SpatialMap[T]): SpatialMap[T] = {
    assert(this.startTime < sm.endTime && this.endTime < sm.startTime,
      "The two spatial maps are not temporally overlapping.")
    val newStartTime = math.min(this.startTime, sm.startTime)
    val newEndTime = math.max(this.endTime, sm.endTime)
    SpatialMap[T](id = this.id + "-" + sm.id,
      timeStamp = (newStartTime, newEndTime),
      contents = this.contents ++ sm.contents)
  }

  def aggregate(otherSMs: Array[SpatialMap[T]],
                id: String = "AggregatedSpatialMap"): SpatialMap[T] = {
    val newStartTime = (otherSMs.map(_.startTime) :+ this.startTime).min
    val newEndTime = (otherSMs.map(_.endTime) :+ this.endTime).max
    SpatialMap[T](id = id,
      timeStamp = (newStartTime, newEndTime),
      contents = Array.concat(this.contents, otherSMs.flatMap(_.contents)))
  }

  def split(num: Int): Array[SpatialMap[T]] = {
    val subSMLength = contents.length / num + 1
    splitByCapacity(subSMLength)
  }

  def splitByCapacity(cap: Int): Array[SpatialMap[T]] = {
    val contentsArray = contents.sliding(cap, cap).toArray
    contentsArray.zipWithIndex.map {
      case (contents, i) =>
        SpatialMap(id = this.id + "-" + i, timeStamp = this.timeStamp, contents = contents)
    }
  }

  def printInfo(): String = {
    var s = s"+++++ SptialMap: ${this.id}\n" +
      s"++ temporal range: ${this.timeStamp}\n" +
      s"++ spatial ranges and number of contents in each range:\n"
    this.contents.foreach(x => s = s ++ x._1.coordinates.mkString("++  (", ",", "): ") + x._2.length + "\n")
    s ++ "+++++\n"
  }
}
