package operators

abstract class OperatorSet {
  /** an OperatorSet has to have the following three operators */
  val selector: _
  val converter: _
  val extractor: _
}
