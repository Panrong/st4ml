package operators

import operators.convertion.Converter
import operators.selection.DefaultSelector

class CustomOperatorSet[S, C, E]
  (override val selector: S, override val converter: C = new Converter, override val extractor: E)
  extends OperatorSet