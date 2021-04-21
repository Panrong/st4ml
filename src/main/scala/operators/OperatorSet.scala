package operators

import operators.convertion.Converter

abstract class OperatorSet extends Serializable {
  val selector: Any
  val converter: Any
  val extractor: Any
}
