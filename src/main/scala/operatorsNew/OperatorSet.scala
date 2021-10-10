package operatorsNew

import converter.Converter
import extractor.Extractor
import instances.Instance
import selector.LegacySelector

abstract class OperatorSet extends Serializable {
  type I <: Instance[_, _, _] // type before conversion
  type O <: Instance[_, _, _] // type after conversion
  val selector: LegacySelector[I]
  val converter: Converter
  val extractor: Extractor[O]
}