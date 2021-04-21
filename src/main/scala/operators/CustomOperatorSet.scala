package operators

class CustomOperatorSet[S, C, E]
  (override val selector: S, override val converter: C, override val extractor: E)
  extends OperatorSet