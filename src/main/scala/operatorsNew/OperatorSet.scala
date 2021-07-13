package operatorsNew

import converter.Converter
import extractor.Extractor
import selector.Selector

abstract class OperatorSet extends Serializable {

  val selector: Selector[_]
  val converter: Converter
  val extractor: Extractor[_,_]
}