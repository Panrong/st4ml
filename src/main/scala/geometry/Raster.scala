package geometry

case class Raster[T](id: String, contents: Array[TimeSeries[T]]) {
  def addSpatial(raster: Raster[T]): Raster[T] = {
    Raster(id = this.id + "-" + raster.id,
      contents = this.contents ++ raster.contents)
  }

  def addTemporal(raster: Raster[T]): Raster[T] = ???

  def AggregateSpatial(rasters: Array[Raster[T]]): Raster[T] = ???

  def AggregateTemporal(rasters: Array[Raster[T]]): Raster[T] = ???

  def splitSpatial(num: Int): Array[Raster[T]] = ???

  def splitTemporal(num: Int): Array[Raster[T]] = ???
}
