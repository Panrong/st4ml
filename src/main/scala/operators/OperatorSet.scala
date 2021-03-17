package operators

abstract class OperatorSet extends Serializable {
  /** an OperatorSet has to have the following three operators */
  val selector: Any
  val converter: Any
  val extractor: Any
}
