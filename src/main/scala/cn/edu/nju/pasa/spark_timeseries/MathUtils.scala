package cn.edu.nju.pasa.spark_timeseries

import scala.math

object MathUtils {

  def sum(x: Array[Double], initVal: Option[Double] = None): Double = {
    x.foldLeft(initVal.getOrElse(0.0))((lhs, rhs) => lhs + rhs)
  }

  def mean(x: Array[Double], sum: Option[Double]=None): Double = {
    val sum_ = sum.getOrElse(MathUtils.sum(x))
    sum_ / x.size
  }

  def std(x: Array[Double], mean: Option[Double]=None): Double = {
    val mean_ = mean.getOrElse(MathUtils.mean(x))
    MathUtils.mean(x.map(e => math.pow(e - mean_, 2)))
  }
}
