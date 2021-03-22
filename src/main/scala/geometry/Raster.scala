package geometry

import scala.reflect.ClassTag

/**
 * a raster is similar to a spatial map while the contents of each spatial region is a TimeSeries
 *
 * @param id       : id of the raster
 * @param contents : each spatial region has element of (id, TimeSeries)
 * @tparam T : type of the TimeSeries contents
 */
case class Raster[T: ClassTag](id: String,
                               contents: Array[TimeSeries[T]]) {
  def addSpatial(raster: Raster[T]): Raster[T] = {
    Raster(id = this.id + "-" + raster.id,
      contents = this.contents ++ raster.contents)
  }

  def addTemporal(raster: Raster[T]): Raster[T] = {
    Raster(id = this.id,
      contents = (this.contents zip raster.contents)
        .map(x => x._1.extend(x._2)))
  }

  def aggregateSpatial(others: Array[Raster[T]]): Raster[T] = {
    others.foldLeft(this)(_.addSpatial(_))
  }

  def aggregateTemporal(others: Array[Raster[T]]): Raster[T] = {
    others.foldLeft(this)(_.addTemporal(_))
  }

  def splitSpatial(num: Int): Array[Raster[T]] = {
    val subRasterLength = contents.length / num + 1
    val contentsArray = contents.sliding(subRasterLength, subRasterLength).toArray
    contentsArray.zipWithIndex.map {
      case (contents, i) =>
        Raster(id = this.id + "-" + i, contents = contents)
    }
  }

  def splitTemporal(num: Int): Array[Raster[T]] = {
    contents.map(content =>
      content.split(num)).zipWithIndex.map {
      case (content, id) =>
        Raster(id = this.id + "-" + id.toString, contents = content)
    }
  }

  def spatialRange(): Rectangle = {
    val recs = contents.map(_.spatialRange)
    val lonMin = recs.map(_.xMin).min
    val latMin = recs.map(_.yMin).min
    val lonMax = recs.map(_.xMax).max
    val latMax = recs.map(_.yMax).max
    Rectangle(Array(lonMin, latMin, lonMax, latMax))
  }

  def temporalRange(): (Long, Long) = {
    (contents.map(_.startTime).min,
      contents.map(_.endTime).max)
  }
}
