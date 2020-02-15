package cn.edu.nju.pasa.spark_timeseries


object ArrayUtils {

  def sum(x: Array[Double], initVal: Option[Double] = None): Double = {
    x.foldLeft(initVal.getOrElse(0.0))((lhs, rhs) => lhs + rhs)
  }

  def mean(x: Array[Double], sum: Option[Double] = None): Double = {
    val sum_ = sum.getOrElse(ArrayUtils.sum(x))
    sum_ / x.length
  }

  def seqLengthWhere(x: Array[Boolean]): Array[Double] = {
    if (x.length == 0)
      Array(0.0)
    else {
      val xEnum = x.zipWithIndex
      xEnum.map(ele => {
        if (ele._1) {
          xEnum.foldLeft(ele._2)((cur, ele) => {
            if (ele._1 && ele._2 == cur + 1) {
              ele._2
            } else {
              cur
            }
          })
        } else {
          -1
        }
      }).filter(_ >= 0).groupBy(each => each).map(_._2.length.toDouble).toArray
    }
  }

  def std(x: Array[Double], mean: Option[Double] = None): Double = {
    val mean_ = mean.getOrElse(ArrayUtils.mean(x))
    sqrt(ArrayUtils.mean(x.map(e => math.pow(e - mean_, 2))))
  }

  def roll(x: Array[Double], shift: Int): Array[Double] = {
    // avoid negative shift
    val idx = (shift % x.length + x.length) % x.length
    x.takeRight(x.length - idx) ++ x.take(idx)
  }

  def diff(x: Array[Double], derivative: Int = 1): Array[Double] = {
    val shift = new Array[Double](derivative) ++ x.take(x.length - derivative)
    x.zip(shift).map(each => each._1 - each._2)
  }

  def normalize(x: Array[Double]): Array[Double] = {
    val mean_ = ArrayUtils.mean(x)
    val std_ = ArrayUtils.std(x, Option(mean_))
    if (std_ == 0.0)
      new Array[Double](x.length)
    else
      x.map(each => (each - mean_) / std_)
  }

  def dot(lhs: Array[Double], rhs: Array[Double]): Double = {
    sum(lhs.zip(rhs).map(each => each._1 * each._2))
  }

  def sqrt(x: Double): Double = {
    math.sqrt(x)
  }

  def sqrt(x: Array[Double]): Array[Double] = {
    x.map(math.sqrt)
  }

  def abs(x: Array[Double]): Array[Double] = {
    x.map(math.abs)
  }

  def where(x: Array[Double], cond: Double => Boolean): Array[Boolean] = {
    x.map(cond)
  }
}
