package instances

import org.locationtech.jts.geom.{GeometryFactory, PrecisionModel}
import org.locationtech.jts.precision.GeometryPrecisionReducer

object GeometryFactory {
  // config
  lazy val precisionType: String = "floating"
  lazy val scale: Double = 1e12

  lazy val precisionModel: PrecisionModel = precisionType match {
    case "floating" => new PrecisionModel()
    case "floating_single" => new PrecisionModel(PrecisionModel.FLOATING_SINGLE)
    case "fixed" => new PrecisionModel(scale)
    case _ => throw new IllegalArgumentException(s"""Unrecognized JTS precision model, ${precisionType}; expected "floating", "floating_single", or "fixed" """)
  }

  lazy val factory: GeometryFactory = new GeometryFactory(precisionModel)
  lazy val simplifier: GeometryPrecisionReducer = new GeometryPrecisionReducer(new PrecisionModel(scale))

}
