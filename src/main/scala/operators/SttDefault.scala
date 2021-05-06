package operators

import geometry.Rectangle
import operators.convertion.Traj2PointConverter
import operators.extraction.PointsAnalysisExtractor
import operators.selection.DefaultSelector

class SttDefault(sQuery:Rectangle, tQuery: (Long, Long)) extends OperatorSet {
  val selector: DefaultSelector = new DefaultSelector(sQuery, tQuery)
  val converter = new Traj2PointConverter()
  val extractor = new PointsAnalysisExtractor()
}
