package geometry

import scala.reflect.ClassTag

/**
 * A spatialMap records the information of a spatial range at a certain time stamp.
 *
 * @param id           : id of the spatialMap
 * @param timeStamp    : the temporal snapshot
 * @param contents     : Anything to be recorded. Can be a list, array, map etc. Better to have an identifier alongside.
 * @param classTag$T$0 : Type of contents
 * @tparam T : Type of contents
 */
case class SpatialMap[T: ClassTag](id: String, timeStamp: Long, contents: Array[T]) {

  def add(sm: SpatialMap[T]): SpatialMap[T] = {
    SpatialMap[T](id = this.id + "-" + sm.id,
      timeStamp = math.max(this.timeStamp, sm.timeStamp),
      contents = this.contents ++ sm.contents)
  }

  def aggregate(otherSMs: Array[SpatialMap[T]], id: String = "AggregatedSpatialMap"): SpatialMap[T] = {
    SpatialMap[T](id = id,
      timeStamp = math.max(this.timeStamp, otherSMs.map(_.timeStamp).max),
      contents = Array.concat(this.contents, otherSMs.flatMap(_.contents)))
  }

  def split(num: Int): Array[SpatialMap[T]] = {
    val subSMLength = contents.length / num + 1
    val contentsArray = contents.sliding(subSMLength, subSMLength).toArray
    contentsArray.zipWithIndex.map {
      case (contents, i) => SpatialMap(id = this.id + "-" + i, timeStamp = this.timeStamp, contents = contents)
    }
  }
}
