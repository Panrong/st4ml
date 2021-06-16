import org.locationtech.jts.{geom => jts}
import java.{time => jTime}

package object instances {
  type Point = jts.Point
  type LineString = jts.LineString
  type Polygon = jts.Polygon
  type Geometry = jts.Geometry

  type Instant = jTime.Instant
}
