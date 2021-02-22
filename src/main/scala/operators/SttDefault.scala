package operators

import operators.convertion.Converter
import operators.extraction.PointsAnalysisExtractor
import operators.selection.DefaultSelector

class SttDefault(numPartitions: Int) extends OperatorSet with Serializable {
   val selector: DefaultSelector = DefaultSelector(numPartitions)
   val converter = new Converter()
   val extractor = new PointsAnalysisExtractor()
}
