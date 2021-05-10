package operators

import geometry.{Point, Rectangle, Trajectory}
import operators.convertion.Traj2PointConverter
import operators.extraction.PointsAnalysisExtractor

class SttDefault(sQuery: Rectangle, tQuery: (Long, Long)) extends OperatorSet(sQuery, tQuery) {
  type I = Trajectory
  type O = Point
  val converter = new Traj2PointConverter()
  val extractor = new PointsAnalysisExtractor()
}
