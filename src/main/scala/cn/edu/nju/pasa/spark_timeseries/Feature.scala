package cn.edu.nju.pasa.spark_timeseries

class Feature(val feature: String) {
  val (name, ops) = Feature.parseFeat(feature)
  /***
    * Generate new feature name by ops
    * @return new feature name
    */
  def this(name: String, ops: List[String]) {
    this(Feature.getFeatName(name, ops))
  }

  override def toString: String = {
    // feature name format
    // e.g. A$sum#roll#diff means feature A uses op sum and op diff
    feature
  }
}

object Feature {
  def getFeatName(name: String, ops: List[String]): String = name + ops.mkString(if (ops.nonEmpty) "$" else "", "#", "")

  def parseFeat(feat: String): (String, List[String]) = {
    val info = feat.split("\\$")
    (info.head, info.tail.toList)
  }
}
