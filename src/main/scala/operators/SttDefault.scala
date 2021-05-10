package operators

import geometry.{Point, Rectangle, Trajectory}
import operators.convertion.Traj2PointConverter
import operators.extraction.PointsAnalysisExtractor
import operators.selection.DefaultSelector

class SttDefault(sQuery: Rectangle, tQuery: (Long, Long)) extends OperatorSet {
  override type I = Trajectory
  override type O = Point
  override val selector: DefaultSelector[I] = new DefaultSelector[I](sQuery, tQuery)
  override val converter = new Traj2PointConverter()
  val extractor = new PointsAnalysisExtractor()
}
