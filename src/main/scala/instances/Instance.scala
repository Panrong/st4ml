package instances

trait Instance[+S, +T, +D] {
  val spatial: S
  val temporal: T
  val data: D

  def extent: Extent = spatial match {
    case geom: Geometry => Extent(geom.getEnvelopeInternal)
    case geomArr: Array[Geometry] => Extent(geomArr.map(x => Extent(x.getEnvelopeInternal)))
    case _ => throw new IllegalArgumentException(
      s"""Unrecognized spatial type when calculating extent of the instance, ${spatial};
         | expected "Geometry" or "Array[Geometry]"""".stripMargin)
  }

  def duration: Duration = temporal match {
    case dur: Duration => dur
    case durArr: Array[Duration] => Duration(durArr)
    case _ => throw new IllegalArgumentException(
      s"""Unrecognized temproal type when calculating duration of the instance, ${temporal};
         | expected "Duration" or "Array[Duration]"""".stripMargin)
  }

}

object Instance {
  implicit def instanceToGeometry[S <: Geometry](f: Instance[S, _, _]): S = f.spatial
}