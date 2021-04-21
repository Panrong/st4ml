package operators

import operators.convertion.LegacyConverter
import operators.selection.DefaultSelector

class CustomOperatorSet[S, C, E]
  (override val selector: S, override val converter: C = new LegacyConverter, override val extractor: E)
  extends OperatorSet