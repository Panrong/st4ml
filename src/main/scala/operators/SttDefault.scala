package operators

import operators.convertion.Traj2PointConverter
import operators.extraction.PointsAnalysisExtractor
import operators.selection.DefaultSelector

class SttDefault(numPartitions: Int) extends OperatorSet {
  val selector: DefaultSelector = DefaultSelector(numPartitions)
  val converter = new Traj2PointConverter()
  val extractor = new PointsAnalysisExtractor()
}
