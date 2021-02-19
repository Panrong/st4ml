package operators

import convertion.Converter
import extraction.PointsAnalysisExtractor
import selection.queryHandler.DefaultQueryHandler

class SttDefault(numPartitions: Int) extends Serializable {
  val queryHandler: DefaultQueryHandler = DefaultQueryHandler(numPartitions)
  val converter = new Converter()
  val extractor = new PointsAnalysisExtractor()
}
