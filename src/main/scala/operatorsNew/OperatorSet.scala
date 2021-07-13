package operatorsNew

import converter.Converter
import extractor.Extractor
import instances.Instance
import selector.Selector

abstract class OperatorSet extends Serializable {
  type I <: Instance[_, _, _] // type before conversion
  type O <: Instance[_, _, _] // type after conversion
  val selector: Selector[I]
  val converter: Converter
  val extractor: Extractor[O]
}