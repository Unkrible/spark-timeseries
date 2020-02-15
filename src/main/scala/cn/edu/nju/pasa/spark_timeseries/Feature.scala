package cn.edu.nju.pasa.spark_timeseries

class Feature(val feature: String, val ops: List[String] = List.empty[String]) {
  /***
    * Generate new feature name by ops
    * @return new feature name
    */
  override def toString: String = {
    // feature name format
    // e.g. A$sum#roll#diff means feature A uses op sum and op diff
    feature + ops.mkString("$", "#", "")
  }
}
